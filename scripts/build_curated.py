"""Build a deduplicated report snapshot from committed processed_v2 batches.

Copy this file to credit_card_project/scripts/build_curated.py and run:
    python scripts/build_curated.py

Reads the existing .env. Does not modify streaming code, source CSVs, the
outbox, processed_v2, or the streaming checkpoint. Intended for one WSL host;
the main() file lock serializes builders on that host. Each run recomputes a
complete snapshot, then switches CURRENT.json to that completed snapshot.
"""

import csv
import fcntl
import json
import os
import re
import shutil
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from functools import reduce
from pathlib import Path

from dotenv import load_dotenv
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F


ROOT = Path(__file__).resolve().parents[1]
CORE_COLUMNS = [
    "offset", "User", "Card", "date_formatted", "time_formatted",
    "transaction_id", "pos_event_time", "currency", "merchant", "city",
    "amount_numeric", "errors", "is_fraud", "transaction_status",
    "error_status", "final_status",
]
FX_COLUMNS = [
    "fx_rate", "fx_retrieved_at", "fx_bank_published_at", "amount_vnd",
]
AUDIT_COLUMNS = ["topic", "partition", "kafka_timestamp", "raw_value", "validation_errors"]
ALL_COLUMNS = CORE_COLUMNS + FX_COLUMNS + AUDIT_COLUMNS
BUSINESS_COLUMNS = [c for c in CORE_COLUMNS if c != "offset"]
GROUP_STATUSES = {
    "success": ["SUCCESS"],
    "fraud": ["REJECTED_FRAUD"],
    "errors": ["REJECTED_ERROR"],
    "invalid": ["INVALID_FLAG", "INVALID_DATA"],
}


class HadoopStore:
    """Small wrapper for Hadoop paths, including atomic HDFS file replacement."""

    def __init__(self, spark, base):
        self.jvm = spark._jvm
        self.gateway = spark.sparkContext._gateway
        self.conf = spark.sparkContext._jsc.hadoopConfiguration()
        self.Path = self.jvm.org.apache.hadoop.fs.Path
        self.fs = self.Path(base).getFileSystem(self.conf)
        self.context = self.jvm.org.apache.hadoop.fs.FileContext.getFileContext(
            self.fs.getUri(), self.conf
        )

    def qualified(self, path):
        return self.fs.makeQualified(self.Path(path)).toString()

    def exists(self, path):
        return self.fs.exists(self.Path(path))

    def children(self, path):
        return self.fs.listStatus(self.Path(path)) if self.exists(path) else []

    def mkdir(self, path):
        if not self.fs.mkdirs(self.Path(path)):
            raise RuntimeError(f"Khong tao duoc thu muc: {path}")

    def write_json(self, path, data):
        stream = self.fs.create(self.Path(path), False)
        try:
            # ensure_ascii makes Java writeBytes safe for every JSON character.
            stream.writeBytes(json.dumps(data, ensure_ascii=True, indent=2) + "\n")
        finally:
            stream.close()

    def read_json(self, path):
        reader = self.jvm.java.io.BufferedReader(
            self.jvm.java.io.InputStreamReader(self.fs.open(self.Path(path)), "UTF-8"))
        try:
            lines = []
            while True:
                line = reader.readLine()
                if line is None:
                    break
                lines.append(line)
            return json.loads("\n".join(lines))
        finally:
            reader.close()

    def csv_header(self, path):
        reader = self.jvm.java.io.BufferedReader(
            self.jvm.java.io.InputStreamReader(self.fs.open(self.Path(path)), "UTF-8"))
        try:
            line = reader.readLine()
            if line is None:
                raise RuntimeError(f"CSV rong: {path}")
            return tuple(next(csv.reader([line])))
        finally:
            reader.close()

    def rename(self, source, target, overwrite=False):
        Rename = self.jvm.org.apache.hadoop.fs.Options.Rename
        options = self.gateway.new_array(Rename, 1)
        options[0] = Rename.OVERWRITE if overwrite else Rename.NONE
        self.context.rename(self.Path(source), self.Path(target), options)

    def remove_own_temporary_path(self, path):
        if self.exists(path):
            self.fs.delete(self.Path(path), True)


def discover_inputs(store, base, checkpoint):
    """Freeze a cutoff: exclude batches whose foreachBatch has not committed.

    Spark prunes older commit logs, so use the greatest committed batch id,
    not membership in the remaining log filenames. This assumes the existing
    checkpoint lineage is retained, as required by this project's workflow.
    """
    committed = [
        int(item.getPath().getName())
        for item in store.children(f"{checkpoint}/commits")
        if item.isFile() and item.getPath().getName().isdigit()
    ]
    if not committed:
        raise RuntimeError("Chua co batch da commit trong checkpoint dang dung.")
    cutoff = max(committed)
    managed = {}
    for item in store.children(f"{base}/processed_v2/_batches"):
        match = re.fullmatch(r"batch_id=(\d+)", item.getPath().getName())
        if not item.isDirectory() or not match or int(match[1]) > cutoff:
            continue
        complete = f"{item.getPath()}/complete.json"
        if not store.exists(complete):
            raise RuntimeError(f"Batch da commit nhung thieu complete.json: {item.getPath()}")
        manifest = store.read_json(complete)
        if manifest.get("format_version") != 1 or manifest["input"]["batch_id"] != int(match[1]):
            raise RuntimeError("Manifest batch khong hop le")
        managed[int(match[1])] = manifest
        for group, record in manifest["groups"].items():
            expected = f"{base}/processed_v2/{group}/{item.getPath().getName()}"
            if (group not in GROUP_STATUSES or record["path"] != store.qualified(expected)
                    or not store.exists(f"{expected}/_SUCCESS")):
                raise RuntimeError(f"Batch da commit nhung CSV bi thieu/sai: {expected}")
    sources = {}
    for group in GROUP_STATUSES:
        files = []
        for item in store.children(f"{base}/processed_v2/{group}"):
            match = re.fullmatch(r"batch_id=(\d+)", item.getPath().getName())
            if not item.isDirectory() or not match or int(match[1]) > cutoff:
                continue
            if int(match[1]) in managed and group not in managed[int(match[1])]["groups"]:
                raise RuntimeError("Thu muc CSV khong khop manifest batch")
            directory = item.getPath().toString()
            if not store.exists(f"{directory}/_SUCCESS"):
                raise RuntimeError(f"Batch da commit nhung CSV chua hoan tat: {directory}")
            parts = [
                child.getPath().toString()
                for child in store.children(directory)
                if child.isFile()
                and child.getPath().getName().startswith("part-")
                and child.getPath().getName().endswith(".csv")
            ]
            if not parts:
                raise RuntimeError(f"Khong tim thay part-*.csv: {directory}")
            files.extend(parts)
        if files:
            sources[group] = sorted(files)
    if not sources:
        raise RuntimeError("Chua co CSV processed_v2 thuoc batch da commit.")
    return cutoff, sources


def read_events(spark, sources):
    parts = []
    for group, paths in sources.items():
        # Legacy and new batches have different headers. Read each schema
        # separately; never apply one CSV header to all historical files.
        store = HadoopStore(spark, paths[0])
        by_header = {}
        for path in paths:
            header = store.csv_header(path)
            by_header.setdefault(header, []).append(path)
        for header, matching_paths in by_header.items():
            missing = set(CORE_COLUMNS) - set(header)
            unexpected = set(header) - set(ALL_COLUMNS)
            if missing or unexpected or len(header) != len(set(header)):
                raise RuntimeError(
                    f"Schema {group} khong khop; thieu={sorted(missing)}, "
                    f"cot chua ho tro={sorted(unexpected)}"
                )
            df = (spark.read.option("header", True).option("multiLine", True)
                  .option("enforceSchema", False).option("mode", "FAILFAST")
                  .option("escape", '"' if "raw_value" in header else "\\")
                  .csv(matching_paths))
            for name in FX_COLUMNS + AUDIT_COLUMNS:
                if name not in df.columns:
                    df = df.withColumn(name, F.lit(None).cast("string"))
            parts.append(
                df.select(*ALL_COLUMNS)
                .withColumn("_source_group", F.lit(group))
                .withColumn("_source_file", F.input_file_name())
                .withColumn("_source_batch_id", F.regexp_extract(
                    F.col("_source_file"), r"/batch_id=(\d+)/", 1).cast("long"))
            )
    return reduce(lambda a, b: a.unionByName(b), parts)


def prepare_transactions(raw):
    missing_id = F.col("transaction_id").isNull() | (F.length(F.trim("transaction_id")) == 0)
    excluded = raw.filter(missing_id)
    valid = raw.filter(~missing_id)

    allowed_group = F.lit(False)
    for group, statuses in GROUP_STATUSES.items():
        allowed_group = allowed_group | (
            (F.col("_source_group") == group) & F.col("final_status").isin(statuses)
        )
    if not raw.filter(~F.coalesce(allowed_group, F.lit(False))).isEmpty():
        raise RuntimeError("final_status khong khop voi thu muc phan loai.")

    # Different contents under one business id must not be silently discarded.
    valid = valid.withColumn("_business_json", F.to_json(
        F.struct(*BUSINESS_COLUMNS), options={"ignoreNullFields": "false"}))
    conflicts = (valid.groupBy("transaction_id")
                 .agg(F.countDistinct("_business_json").alias("versions"))
                 .filter(F.col("versions") > 1).limit(5).collect())
    if conflicts:
        ids = ", ".join(row["transaction_id"] for row in conflicts)
        raise RuntimeError(f"Cung transaction_id nhung khac noi dung: {ids}")

    valid = (valid.withColumn("_source_offset", F.col("offset").cast("decimal(20,0)"))
             .withColumn("amount_numeric", F.col("amount_numeric").cast("decimal(18,2)"))
             .withColumn("fx_rate", F.col("fx_rate").cast("decimal(18,4)"))
             .withColumn("amount_vnd", F.col("amount_vnd").cast("decimal(24,2)")))
    if not valid.filter(
        F.col("_source_batch_id").isNull() | F.col("_source_offset").isNull()
        | (F.col("_source_offset") < 0)
    ).isEmpty():
        raise RuntimeError("Khong xac dinh duoc batch hoac Kafka offset hop le.")

    # Replays can carry a newer FX rate: consistently keep the earliest stored
    # copy. Never reload today's FX rate while rebuilding historical reports.
    key_window = Window.partitionBy("transaction_id")
    ordered = key_window.orderBy("_source_batch_id", "_source_offset", "_source_file")
    transactions = (valid.withColumn("delivery_count", F.count("*").over(key_window))
                    .withColumn("_rank", F.row_number().over(ordered))
                    .filter(F.col("_rank") == 1)
                    .select(*ALL_COLUMNS, "delivery_count",
                            F.col("_source_batch_id").alias("first_seen_batch_id")))
    success = F.col("final_status") == "SUCCESS"
    bad_money = (
        F.col("currency").isNull() | (F.col("currency") != "USD")
        | F.col("amount_numeric").isNull() | F.col("fx_rate").isNull()
        | (F.col("fx_rate") <= 0) | F.col("amount_vnd").isNull()
        | (F.col("amount_vnd") != (F.col("amount_numeric") * F.col("fx_rate")).cast("decimal(24,2)"))
    )
    if not transactions.filter(success & bad_money).isEmpty():
        raise RuntimeError("Giao dich SUCCESS thieu hoac sai so tien/ty gia USD-VND.")
    return transactions, excluded


def build_snapshot(spark, base, checkpoint):
    store = HadoopStore(spark, base)
    cutoff, sources = discover_inputs(store, base, checkpoint)
    raw = read_events(spark, sources).persist(StorageLevel.MEMORY_AND_DISK)
    transactions = None
    temporary = None
    pointer_temporary = None
    try:
        source_rows = raw.count()
        # Check actual CSV counts against each managed batch, not just _SUCCESS.
        manifests = {}
        for row in raw.groupBy("_source_batch_id", "_source_group").count().collect():
            batch_id = row["_source_batch_id"]
            path = f"{base}/processed_v2/_batches/batch_id={batch_id:010d}/complete.json"
            if batch_id not in manifests:
                manifests[batch_id] = store.read_json(path) if store.exists(path) else None
            manifest = manifests[batch_id]
            if manifest and row["count"] != manifest["groups"][row["_source_group"]]["rows"]:
                raise RuntimeError(f"So dong CSV khong khop manifest batch {batch_id}")
        transactions, excluded = prepare_transactions(raw)
        transactions = transactions.persist(StorageLevel.MEMORY_AND_DISK)
        unique_rows = transactions.count()
        excluded_rows = excluded.count()
        if unique_rows == 0:
            raise RuntimeError("Chua co giao dich co transaction_id de tao bao cao.")
        statuses = {r["final_status"]: r["count"]
                    for r in transactions.groupBy("final_status").count().collect()}
        sums = (transactions.filter(F.col("final_status") == "SUCCESS")
                .agg(F.sum("amount_numeric").alias("usd"),
                     F.sum("amount_vnd").alias("vnd")).first())
        now = datetime.now(timezone.utc)
        snapshot_id = now.strftime("%Y%m%dT%H%M%SZ_") + uuid.uuid4().hex[:12]
        root = f"{base}/curated"
        temporary = f"{root}/_staging/{snapshot_id}"
        final = f"{root}/snapshots/{snapshot_id}"
        pointer_temporary = f"{root}/.CURRENT_{snapshot_id}.tmp"
        store.mkdir(temporary)
        store.mkdir(f"{root}/snapshots")
        manifest = {
            "format_version": 1, "snapshot_id": snapshot_id,
            "built_at_utc": now.isoformat(), "source_committed_batch": cutoff,
            "source_rows": source_rows,
            "source_file_count": sum(map(len, sources.values())),
            "missing_id_rows": excluded_rows,
            "duplicate_extra_rows": source_rows - excluded_rows - unique_rows,
            "curated_transactions": unique_rows, "status_counts": statuses,
            "success_amount_usd": str(sums["usd"] or Decimal("0.00")),
            "success_amount_vnd": str(sums["vnd"] or Decimal("0.00")),
            "snapshot_path": store.qualified(final),
            "transactions_path": store.qualified(f"{final}/transactions"),
        }
        # One CSV part is convenient for this small teaching dataset. All
        # statuses remain available; only SUCCESS contributes to money totals.
        (transactions.coalesce(1).write.mode("errorifexists")
         .option("header", True).option("escape", '"').csv(f"{temporary}/transactions"))
        if excluded_rows:
            (excluded.select(*ALL_COLUMNS).coalesce(1).write.mode("errorifexists")
             .option("header", True).option("escape", '"').csv(f"{temporary}/excluded_missing_id"))
        store.write_json(f"{temporary}/manifest.json", manifest)
        store.rename(temporary, final)
        # Never delete CURRENT before replacing it. On HDFS the rename-overwrite
        # switches the pointer as one namespace operation after the data exists.
        store.write_json(pointer_temporary, manifest)
        store.rename(pointer_temporary, f"{root}/CURRENT.json", overwrite=True)
        return manifest
    finally:
        raw.unpersist()
        if transactions is not None:
            transactions.unpersist()
        # These paths belong only to this run. Published snapshots are retained.
        for path in (temporary, pointer_temporary):
            if path:
                store.remove_own_temporary_path(path)


def main():
    load_dotenv(ROOT / ".env")
    if not os.getenv("HADOOP_CONF_DIR"):
        command = shutil.which("hdfs")
        if command:
            directory = Path(command).resolve().parents[1] / "etc" / "hadoop"
            if directory.is_dir():
                os.environ["HADOOP_CONF_DIR"] = str(directory)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    base = os.environ["HDFS_BASE_PATH"].rstrip("/")
    checkpoint = os.environ["HDFS_CHECKPOINT_PATH"].rstrip("/")
    if not base or not checkpoint:
        raise RuntimeError("Can HDFS_BASE_PATH va HDFS_CHECKPOINT_PATH hop le.")

    lock_path = ROOT / "data" / "curated_build.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Mot chuong trinh build_curated khac dang chay.") from None
        spark = (SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[2]"))
                 .appName("CreditCardCuratedSnapshot")
                 .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        try:
            if HadoopStore(spark, base).fs.getUri().getScheme() != "hdfs":
                raise RuntimeError("Spark chua ket noi HDFS. Kiem tra HADOOP_CONF_DIR.")
            result = build_snapshot(spark, base, checkpoint)
            for key in ("source_committed_batch", "source_rows", "missing_id_rows",
                        "duplicate_extra_rows", "curated_transactions",
                        "success_amount_usd", "success_amount_vnd"):
                print(f"{key.upper()}: {result[key]}", flush=True)
            for status, count in sorted(result["status_counts"].items()):
                print(f"{status}: {count}", flush=True)
            print(f"CURATED_DATA: {result['transactions_path']}", flush=True)
            print(f"CURRENT_MANIFEST: {base}/curated/CURRENT.json", flush=True)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
