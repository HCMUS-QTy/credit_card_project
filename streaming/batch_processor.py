"""Xử lý micro-batch theo các bước rõ ràng, giữ nguyên kết quả khi retry."""

from decimal import Decimal

from pyspark import StorageLevel
from pyspark.sql import DataFrame, functions as F

from scripts.build_curated import GROUP_STATUSES, HadoopStore
from .batch_storage import BatchPaths, publish_json
from .fx_rates import validate_fx_record


class BatchProcessor:
    def __init__(self, spark, base, rate_provider, lineage_id):
        self.base = base.rstrip("/")
        self.store = HadoopStore(spark, self.base)
        self.rate_provider = rate_provider
        self.lineage_id = lineage_id

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Kiểm tra input → cố định tỷ giá → ghi CSV → đánh dấu hoàn tất."""
        if batch_df.isEmpty():
            return

        transactions = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
        paths = BatchPaths(self.base, batch_id)
        try:
            identity = self.input_identity(transactions, batch_id)
            self._register_input(paths, identity)

            status_counts = self._count_statuses(transactions)
            group_counts = self._count_groups(status_counts)
            rate_record = self._load_batch_rate(paths, group_counts["success"] > 0)
            manifest = self._build_manifest(paths, identity, status_counts, group_counts, rate_record)

            if self._is_completed(paths, manifest):
                print(f"Batch {batch_id}: da hoan tat, giu nguyen CSV", flush=True)
                return

            converted = self._convert_success_amounts(transactions, rate_record)
            self._publish_groups(paths, converted, group_counts)
            publish_json(self.store, paths.metadata_file("complete.json"), manifest)
            print(f"Batch {batch_id} completed: {status_counts}", flush=True)
        finally:
            transactions.unpersist()

    def input_identity(self, transactions: DataFrame, batch_id: int) -> dict:
        invalid_position = (
            F.col("topic").isNull()
            | F.col("partition").isNull()
            | F.col("offset").isNull()
            | (F.col("partition") < 0)
            | (F.col("offset") < 0)
        )
        if not transactions.filter(invalid_position).isEmpty():
            raise RuntimeError("Thieu topic/partition/offset hop le")

        row_hash = F.xxhash64("topic", "partition", "offset", "raw_value")
        summary = transactions.agg(
            F.count("*").alias("rows"),
            F.sum(row_hash.cast("decimal(38,0)")).alias("fingerprint"),
        ).first()
        partition_offsets = (
            transactions.groupBy("topic", "partition")
            .agg(
                F.min("offset").alias("first_offset"),
                F.max("offset").alias("last_offset"),
                F.count("*").alias("rows"),
            )
            .orderBy("topic", "partition")
            .collect()
        )
        return {
            "format_version": 1,
            "pipeline_version": 1,
            "lineage_id": self.lineage_id,
            "batch_id": batch_id,
            "row_count": summary["rows"],
            "fingerprint": str(summary["fingerprint"]),
            "offsets": [row.asDict() for row in partition_offsets],
        }

    def _register_input(self, paths: BatchPaths, identity: dict) -> None:
        input_path = paths.metadata_file("input.json")
        if self.store.exists(input_path):
            if self.store.read_json(input_path) != identity:
                raise RuntimeError("Batch ID da ton tai voi input/lineage khac")
            return

        existing_outputs = any(
            self.store.exists(paths.group_directory(group))
            for group in GROUP_STATUSES
        )
        if existing_outputs:
            raise RuntimeError("Batch cu chua co metadata; khong tu dong ghi de.")
        publish_json(self.store, input_path, identity)

    def _count_statuses(self, transactions: DataFrame) -> dict:
        rows = transactions.groupBy("final_status").count().collect()
        status_counts = {row["final_status"]: row["count"] for row in rows}
        allowed_statuses = {
            status
            for statuses in GROUP_STATUSES.values()
            for status in statuses
        }
        if set(status_counts) - allowed_statuses:
            raise RuntimeError("Batch co final_status khong duoc ho tro")
        return status_counts

    def _count_groups(self, status_counts: dict) -> dict:
        return {
            group: sum(status_counts.get(status, 0) for status in statuses)
            for group, statuses in GROUP_STATUSES.items()
        }

    def _load_batch_rate(self, paths: BatchPaths, has_success: bool) -> dict | None:
        if not has_success:
            return None

        rate_path = paths.metadata_file("fx.json")
        if self.store.exists(rate_path):
            record = self.store.read_json(rate_path)
            # Retry qua ngày vẫn dùng tỷ giá đã cố định cho batch này.
            validate_fx_record(record, self.rate_provider.rate_type, require_today=False)
            return record

        record = self.rate_provider.get_record()
        validate_fx_record(record, self.rate_provider.rate_type)
        publish_json(self.store, rate_path, record)
        return record

    def _build_manifest(
        self,
        paths: BatchPaths,
        identity: dict,
        status_counts: dict,
        group_counts: dict,
        rate_record: dict | None,
    ) -> dict:
        published_groups = {}
        for group, row_count in group_counts.items():
            if row_count == 0:
                continue
            published_groups[group] = {
                "rows": row_count,
                "path": self.store.qualified(paths.group_directory(group)),
            }
        return {
            "format_version": 1,
            "input": identity,
            "status_counts": status_counts,
            "groups": published_groups,
            "fx": rate_record,
        }

    def _is_completed(self, paths: BatchPaths, manifest: dict) -> bool:
        complete_path = paths.metadata_file("complete.json")
        if not self.store.exists(complete_path):
            return False
        if self.store.read_json(complete_path) != manifest:
            raise RuntimeError("Manifest batch khong khop input hien tai")
        for group in manifest["groups"]:
            self._require_complete_csv(paths.group_directory(group))
        return True

    def _convert_success_amounts(
        self, transactions: DataFrame, rate_record: dict | None,
    ) -> DataFrame:
        rate = None
        retrieved_at = None
        published_at = None
        if rate_record is not None:
            rate = Decimal(rate_record["rate"])
            retrieved_at = rate_record["retrieved_at"]
            published_at = rate_record["bank_published_at"]

        success = F.col("final_status") == "SUCCESS"
        converted = transactions.withColumns({
            "fx_rate": F.when(success, F.lit(rate)).cast("decimal(18,4)"),
            "fx_retrieved_at": F.when(success, F.lit(retrieved_at)).cast("string"),
            "fx_bank_published_at": F.when(success, F.lit(published_at)).cast("string"),
        })
        converted = converted.withColumn(
            "amount_vnd",
            F.when(success, F.expr("try_cast(amount_numeric * fx_rate as decimal(24,2))")),
        )
        missing_conversion = F.col("fx_rate").isNull() | F.col("amount_vnd").isNull()
        if not converted.filter(success & missing_conversion).isEmpty():
            raise RuntimeError("So tien quy doi bi thieu hoac vuot decimal(24,2)")
        return converted

    def _publish_groups(
        self, paths: BatchPaths, transactions: DataFrame, group_counts: dict,
    ) -> None:
        staging_root = paths.new_staging_directory()
        try:
            for group, statuses in GROUP_STATUSES.items():
                if group_counts[group] == 0:
                    continue
                destination = paths.group_directory(group)
                if self.store.exists(destination):
                    # Nhóm đã publish trước lần lỗi: giữ nguyên để retry không ghi đè.
                    self._require_complete_csv(destination)
                    continue

                staging_path = f"{staging_root}/{group}"
                group_transactions = transactions.filter(F.col("final_status").isin(statuses))
                self._write_csv(group_transactions, staging_path)
                self.store.mkdir(f"{paths.processed_root}/{group}")
                self.store.rename(staging_path, destination)
        finally:
            self.store.remove_own_temporary_path(staging_root)

    def _require_complete_csv(self, directory: str) -> None:
        if not self.store.exists(f"{directory}/_SUCCESS"):
            raise RuntimeError(f"CSV batch chua hoan tat hoac bi thieu: {directory}")

    def _write_csv(self, transactions: DataFrame, destination: str) -> None:
        (
            transactions.write
            .mode("errorifexists")
            .option("header", True)
            .option("escape", '"')
            .csv(destination)
        )
