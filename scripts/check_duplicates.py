
import os
from functools import reduce
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

base = os.environ["HDFS_BASE_PATH"].rstrip("/")

spark = (
    SparkSession.builder
    .master(os.getenv("SPARK_MASTER", "local[2]"))
    .appName("CheckCreditCardDuplicates")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

keys = [
    "User", "Card", "date_formatted",
    "time_formatted", "merchant", "city",
    "amount_numeric", "errors", "is_fraud"
]

try:
    parts = []

    for group in ("success", "fraud", "errors"):
        path = (
            f"{base}/processed/{group}/"
            "batch_id=*/part-*.csv"
        )

        df = spark.read.option("header", True).csv(path)

        parts.append(
            df.select("offset", *keys)
            .withColumn("hdfs_group", F.lit(group))
        )

    transactions = reduce(
        lambda a, b: a.unionByName(b), parts
    ).cache()

    print("Tong ban ghi:", transactions.count())

    duplicates = (
        transactions.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
    )

    print("So nhom co the trung:", duplicates.count())

    extra = duplicates.select(
        F.sum(F.col("count") - 1).alias("extra")
    ).first()["extra"]

    print("So dong lap theo bo truong:", extra or 0)

    duplicates.orderBy(F.desc("count")).select(
        "User", "Card", "date_formatted",
        "time_formatted", "merchant",
        "amount_numeric", "count"
    ).show(10, truncate=False)

    repeated_offsets = (
        transactions.groupBy("offset")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print("So offset lap:", repeated_offsets)

finally:
    spark.stop()
