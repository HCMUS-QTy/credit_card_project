
import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

base = os.environ["HDFS_BASE_PATH"].rstrip("/")

spark = (
    SparkSession.builder
    .master(os.getenv("SPARK_MASTER", "local[2]"))
    .appName("VerifyCreditCardHDFS")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    for group in ("success", "fraud", "errors"):
        pattern = (
            f"{base}/processed/"
            f"{group}/batch_id=*/part-*.csv"
        )

        df = (
            spark.read
            .option("header", "true")
            .csv(pattern)
        )

        print(
            f"{group.upper()}: {df.count()} giao dich",
            flush=True
        )

        df.select(
            "User", "Card", "final_status"
        ).show(3, truncate=False)

    print("INVALID: Chua co file du lieu")

finally:
    spark.stop()
