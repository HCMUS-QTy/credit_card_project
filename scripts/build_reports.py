"""Build reports from the single snapshot selected by curated/CURRENT.json.

Run from the project root on the same WSL host as build_curated.py:
    python scripts/build_reports.py
    python scripts/build_reports.py --top-n 20

Uses the existing .env and HadoopStore. Each run publishes a complete report
snapshot, then atomically replaces reports/CURRENT.json on HDFS. Money comes
from the stored SUCCESS rows; historical FX rates are never refreshed here.
"""

import argparse
import fcntl
import json
import os
import re
import shutil
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

if __package__:
    from .build_curated import ROOT, HadoopStore
else:
    from build_curated import ROOT, HadoopStore


STATUS_COLUMNS = {
    "SUCCESS": "success_count",
    "REJECTED_FRAUD": "rejected_fraud_count",
    "REJECTED_ERROR": "rejected_error_count",
    "INVALID_FLAG": "invalid_flag_count",
    "INVALID_DATA": "invalid_data_count",
}
REQUIRED_COLUMNS = {
    "transaction_id", "final_status", "date_formatted", "merchant",
    "currency", "amount_numeric", "amount_vnd",
}


def read_json(store, path):
    if not store.exists(path):
        raise RuntimeError(f"Khong tim thay {path}. Hay chay build_curated.py truoc.")
    reader = store.jvm.java.io.BufferedReader(
        store.jvm.java.io.InputStreamReader(store.fs.open(store.Path(path)), "UTF-8")
    )
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


def current_snapshot(store, base):
    # Read CURRENT exactly once. A concurrent curated build may advance it;
    # this run continues reading the immutable snapshot selected here.
    source = read_json(store, f"{base}/curated/CURRENT.json")
    snapshot_id = source.get("snapshot_id", "")
    if source.get("format_version") != 1 or not re.fullmatch(r"[A-Za-z0-9_-]+", snapshot_id):
        raise RuntimeError("Manifest curated khong dung dinh dang version 1.")
    snapshot_path = f"{base}/curated/snapshots/{snapshot_id}"
    if (source.get("snapshot_path") != store.qualified(snapshot_path)
            or source.get("transactions_path") != store.qualified(f"{snapshot_path}/transactions")):
        raise RuntimeError("Duong dan snapshot trong CURRENT.json khong khop.")
    if source != read_json(store, f"{snapshot_path}/manifest.json"):
        raise RuntimeError("CURRENT.json khong khop manifest cua snapshot.")
    if not store.exists(f"{source['transactions_path']}/_SUCCESS"):
        raise RuntimeError("Snapshot curated chua ghi CSV hoan tat.")
    return source


def read_transactions(spark, source):
    raw = (spark.read.option("header", True).option("mode", "FAILFAST")
           .option("enforceSchema", False).csv(source["transactions_path"]))
    missing = REQUIRED_COLUMNS - set(raw.columns)
    if missing:
        raise RuntimeError(f"Curated thieu cot: {sorted(missing)}")
    return (raw.select(*sorted(REQUIRED_COLUMNS))
            .withColumn("amount_numeric", F.col("amount_numeric").cast("decimal(18,2)"))
            .withColumn("amount_vnd", F.col("amount_vnd").cast("decimal(24,2)"))
            .withColumn("transaction_date", F.coalesce(
                F.date_format(F.to_date("date_formatted", "dd/MM/yyyy"), "yyyy-MM-dd"),
                F.lit("UNKNOWN_DATE")))
            .withColumn("merchant", F.when(
                F.col("merchant").isNull() | (F.trim("merchant") == ""),
                F.lit("UNKNOWN_MERCHANT")).otherwise(F.trim("merchant"))))


def money_totals():
    success = F.col("final_status") == "SUCCESS"
    return [F.coalesce(F.sum(F.when(success, F.col(column)).otherwise(0)), F.lit(0))
            .cast("decimal(38,2)").alias(label)
            for column, label in (("amount_numeric", "success_amount_usd"),
                                  ("amount_vnd", "success_amount_vnd"))]


def metrics():
    return [F.count("*").alias("transaction_count"), *[
        F.coalesce(F.sum(F.when(F.col("final_status") == status, 1).otherwise(0)), F.lit(0))
        .cast("long").alias(column) for status, column in STATUS_COLUMNS.items()
    ], *money_totals()]


def validate_and_summarize(transactions, source):
    missing_id = F.col("transaction_id").isNull() | (F.trim("transaction_id") == "")
    bad_status = ~F.coalesce(F.col("final_status").isin(list(STATUS_COLUMNS)), F.lit(False))
    bad_money = (F.col("final_status") == "SUCCESS") & (
        F.col("currency").isNull() | (F.col("currency") != "USD")
        | F.col("amount_numeric").isNull() | F.col("amount_vnd").isNull()
    )
    summary = transactions.agg(
        *metrics(), F.countDistinct("transaction_id").alias("_unique_ids"),
        F.count(F.when(missing_id | bad_status | bad_money, 1)).alias("_invalid_rows"),
        F.count(F.when(F.col("transaction_date") == "UNKNOWN_DATE", 1))
        .alias("unknown_date_transactions"),
    ).first().asDict()
    if summary.pop("_invalid_rows") or summary.pop("_unique_ids") != summary["transaction_count"]:
        raise RuntimeError("Curated co ID trung/thieu, trang thai hoac so tien SUCCESS khong hop le.")
    if summary["transaction_count"] != source["curated_transactions"]:
        raise RuntimeError("So giao dich khong khop manifest curated.")
    summary["status_counts"] = {
        status: summary[column] for status, column in STATUS_COLUMNS.items()
    }
    expected = {status: source["status_counts"].get(status, 0) for status in STATUS_COLUMNS}
    if summary["status_counts"] != expected:
        raise RuntimeError("So luong theo trang thai khong khop manifest curated.")
    for name in ("success_amount_usd", "success_amount_vnd"):
        if summary[name] != Decimal(source[name]):
            raise RuntimeError(f"Tong {name} khong khop manifest curated.")
        summary[name] = format(summary[name], ".2f")
    for name in ("source_rows", "missing_id_rows", "duplicate_extra_rows"):
        summary[name] = source[name]
    return summary


def report_tables(spark, transactions, top_n):
    statuses = spark.createDataFrame([(s,) for s in STATUS_COLUMNS], ["final_status"])
    by_status = (statuses.join(transactions.groupBy("final_status").agg(
        F.count("*").alias("transaction_count"), *money_totals()), "final_status", "left")
        .fillna({"transaction_count": 0, "success_amount_usd": 0, "success_amount_vnd": 0})
        .orderBy("final_status"))
    by_day = transactions.groupBy("transaction_date").agg(*metrics()).orderBy("transaction_date")
    top_merchants = (transactions.groupBy("merchant").agg(*metrics())
                     .filter(F.col("success_count") > 0)
                     .orderBy(F.desc("success_amount_vnd"), F.desc("success_amount_usd"), "merchant")
                     .limit(top_n))
    dated = (
        transactions
        .withColumn(
            "transaction_month",
            F.when(
                F.col("transaction_date") == "UNKNOWN_DATE",
                F.lit("UNKNOWN_MONTH"),
            ).otherwise(F.substring("transaction_date", 1, 7)),
        )
        .withColumn(
            "transaction_year",
            F.when(
                F.col("transaction_date") == "UNKNOWN_DATE",
                F.lit("UNKNOWN_YEAR"),
            ).otherwise(F.substring("transaction_date", 1, 4)),
        )
    )

    tables = {
        "by_status": by_status,
        "by_day": by_day,
        "top_merchants": top_merchants,
    }

    for name, period in (
        ("merchant_by_day", "transaction_date"),
        ("merchant_by_month", "transaction_month"),
        ("merchant_by_year", "transaction_year"),
    ):
        tables[name] = (
            dated.groupBy("merchant", period)
            .agg(*metrics())
            .orderBy(period, "merchant")
        )

    return tables

def build_reports(spark, base, top_n=10):
    if top_n < 1:
        raise ValueError("top_n phai >= 1")
    store = HadoopStore(spark, base)
    source = current_snapshot(store, base)
    transactions = read_transactions(spark, source).persist(StorageLevel.MEMORY_AND_DISK)
    temporary = pointer_temporary = None
    try:
        summary = validate_and_summarize(transactions, source)
        now = datetime.now(timezone.utc)
        report_id = now.strftime("%Y%m%dT%H%M%SZ_") + uuid.uuid4().hex[:12]
        root = f"{base}/reports"
        temporary = f"{root}/_staging/{report_id}"
        final = f"{root}/snapshots/{report_id}"
        pointer_temporary = f"{root}/.CURRENT_{report_id}.tmp"
        metadata = {
            "format_version": 1, "report_id": report_id, "built_at_utc": now.isoformat(),
            "source_snapshot_id": source["snapshot_id"],
            "source_committed_batch": source["source_committed_batch"],
        }
        summary.update(metadata)
        tables = report_tables(spark, transactions, top_n)
        manifest = {
            **metadata, "source_transactions_path": source["transactions_path"], "top_n": top_n,
            "paths": {name: store.qualified(f"{final}/{name}") for name in tables},
        }
        manifest["paths"]["summary"] = store.qualified(f"{final}/summary.json")
        store.mkdir(temporary)
        store.mkdir(f"{root}/snapshots")
        for name, table in tables.items():
            table = (table.withColumn("source_snapshot_id", F.lit(source["snapshot_id"]))
                     .select("source_snapshot_id", *table.columns))
            (table.coalesce(1).write.mode("errorifexists").option("header", True)
             .csv(f"{temporary}/{name}"))
        store.write_json(f"{temporary}/summary.json", summary)
        store.write_json(f"{temporary}/manifest.json", manifest)
        store.rename(temporary, final)
        store.write_json(pointer_temporary, manifest)
        store.rename(pointer_temporary, f"{root}/CURRENT.json", overwrite=True)
        return manifest, summary
    finally:
        transactions.unpersist()
        for path in (temporary, pointer_temporary):
            if path:
                store.remove_own_temporary_path(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top-n", type=int, default=10,
                        help="So merchant co tong VND thanh cong cao nhat (mac dinh: 10)")
    args = parser.parse_args()
    if args.top_n < 1:
        parser.error("--top-n phai >= 1")
    load_dotenv(ROOT / ".env")
    base = os.getenv("HDFS_BASE_PATH", "").rstrip("/")
    if not base:
        parser.error("Can HDFS_BASE_PATH trong .env")
    if not os.getenv("HADOOP_CONF_DIR"):
        command = shutil.which("hdfs")
        if command:
            directory = Path(command).resolve().parents[1] / "etc" / "hadoop"
            if directory.is_dir():
                os.environ["HADOOP_CONF_DIR"] = str(directory)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    lock_path = ROOT / "data" / "reports_build.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Mot chuong trinh build_reports khac dang chay.") from None
        spark = (SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[2]"))
                 .appName("CreditCardReports").config("spark.sql.shuffle.partitions", "2")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        try:
            if HadoopStore(spark, base).fs.getUri().getScheme() != "hdfs":
                raise RuntimeError("Spark chua ket noi HDFS. Kiem tra HADOOP_CONF_DIR.")
            manifest, summary = build_reports(spark, base, args.top_n)
            print(f"SOURCE_SNAPSHOT_ID: {manifest['source_snapshot_id']}", flush=True)
            print(f"SOURCE_COMMITTED_BATCH: {manifest['source_committed_batch']}", flush=True)
            for key in ("transaction_count", "source_rows", "missing_id_rows", "duplicate_extra_rows",
                        "success_count", "success_amount_usd", "success_amount_vnd"):
                print(f"{key.upper()}: {summary[key]}", flush=True)
            for status, count in summary["status_counts"].items():
                print(f"{status}: {count}", flush=True)
            for name, path in manifest["paths"].items():
                print(f"REPORT_{name.upper()}: {path}", flush=True)
            print(f"REPORTS_CURRENT: {base}/reports/CURRENT.json", flush=True)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
