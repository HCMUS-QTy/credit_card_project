
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, regexp_replace, to_date,
    concat_ws, lpad, date_format, concat, lit,upper, trim, when
)
from pyspark.sql.types import StructType, StructField, StringType
import os
from pathlib import Path
from dotenv import load_dotenv
import json
from decimal import Decimal
from datetime import datetime
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

HDFS_BASE_PATH = os.environ["HDFS_BASE_PATH"].rstrip("/")
HDFS_CHECKPOINT_PATH = os.environ["HDFS_CHECKPOINT_PATH"]

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")

FX_RATE_FILE = PROJECT_ROOT / os.environ["FX_RATE_FILE"]

def load_fx_rate():
    data = json.loads(
        FX_RATE_FILE.read_text(encoding="utf-8")
    )

    if (
        data.get("base_currency") != os.environ["FX_BASE_CURRENCY"]
        or data.get("quote_currency") != os.environ["FX_QUOTE_CURRENCY"]
        or data.get("rate_type") != os.environ["FX_RATE_TYPE"]
    ):
        raise RuntimeError("Cau hinh ty gia khong khop .env")

    rate = Decimal(data["rate"])

    if not rate.is_finite() or rate <= 0:
        raise RuntimeError("Ty gia khong hop le")

    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    retrieved = datetime.fromisoformat(
        data["retrieved_at"]
    ).astimezone(tz)

    if retrieved.date() != datetime.now(tz).date():
        raise RuntimeError(
            "Ty gia da cu. Hay cap nhat usd_vnd_rate.json"
        )

    return (
    rate,
    data["retrieved_at"],
    data.get("bank_published_at")
)


columns = [
    "User", "Card", "Year", "Month", "Day", "Time",
    "Amount", "Use Chip", "Merchant Name", "Merchant City",
    "Merchant State", "Zip", "MCC", "Errors?", "Is Fraud?",
    "transaction_id", "pos_event_time", "currency"
]

schema = StructType([
    StructField(c, StringType(), True) for c in columns
])

spark = (
    SparkSession.builder
    .appName("CreditCardParse")
    .master(SPARK_MASTER)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.select(
        col("offset"),
        from_json(col("value").cast("string"), schema).alias("tx")
    )
    .select("offset", "tx.*")
)

cleaned = (
    parsed
    .withColumn(
        "amount_numeric",
        regexp_replace(col("Amount"), r"[$,]", "")
        .cast("decimal(18,2)")
    )
    .withColumn(
        "transaction_date",
        to_date(
            concat_ws(
                "-",
                lpad(col("Year"), 4, "0"),
                lpad(col("Month"), 2, "0"),
                lpad(col("Day"), 2, "0")
            ),
            "yyyy-MM-dd"
        )
    )
    .withColumn(
        "date_formatted",
        date_format(col("transaction_date"), "dd/MM/yyyy")
    )
    .withColumn(
        "time_formatted",
        concat(col("Time"), lit(":00"))
    )
)

result = cleaned.select(
    "offset",
    "User",
    "Card",
    "date_formatted",
    "time_formatted",
     "transaction_id",
    "pos_event_time",
    "currency",
    col("Merchant Name").alias("merchant"),
    col("Merchant City").alias("city"),
    "amount_numeric",col("Errors?").alias("errors"),
    col("Is Fraud?").alias("is_fraud")
)


classified = (
    result.withColumn(
        "transaction_status",
        when(
            upper(trim(col("is_fraud"))) == "YES",
            "FRAUD"
        )
        .when(
            upper(trim(col("is_fraud"))) == "NO",
            "NON_FRAUD"
        )
        .otherwise("INVALID_FLAG")
    ).withColumn(
    "error_status",
    when(
        col("errors").isNull() |
        (trim(col("errors")) == ""),
        "NO_ERROR"
    ).otherwise("HAS_ERROR")
)
)

finalized = (
    classified.withColumn(
        "final_status",
        when(
            col("transaction_status") == "FRAUD",
            "REJECTED_FRAUD"
        )
        .when(
            col("transaction_status") == "INVALID_FLAG",
            "INVALID_FLAG"
        )
        .when(
            col("date_formatted").isNull() |
            col("amount_numeric").isNull(),
            "INVALID_DATA"
        )
        .when(
            col("error_status") == "HAS_ERROR",
            "REJECTED_ERROR"
        )
        .otherwise("SUCCESS")
    )
)


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"\n========== BATCH {batch_id} ==========")

    batch_df.cache()

    try:
        # Phân loại giao dịch
        batch_df.select(
            "transaction_id", "User", "Card", "final_status"
        ).show(5, truncate=False)
        
        success_df = batch_df.filter(
            col("final_status") == "SUCCESS"
        )

        fraud_df = batch_df.filter(
            col("final_status") == "REJECTED_FRAUD"
        )

        error_df = batch_df.filter(
            col("final_status") == "REJECTED_ERROR"
        )

        invalid_df = batch_df.filter(
            col("final_status").isin(
                "INVALID_FLAG", "INVALID_DATA"
            )
        )

        # Chỉ quy đổi giao dịch thành công
        if not success_df.isEmpty():
            rate, retrieved_at, published_at = load_fx_rate()

            success_df = (
                success_df
                .withColumn(
                    "fx_rate",
                    lit(rate).cast("decimal(18,4)")
                )
                .withColumn(
                    "fx_retrieved_at",
                    lit(retrieved_at)
                )
                .withColumn(
                    "fx_bank_published_at",
                    lit(published_at or "").cast("string")
                )
                .withColumn(
                    "amount_vnd",
                    (
                        col("amount_numeric") * col("fx_rate")
                    ).cast("decimal(24,2)")
                )
            )

        # Lưu riêng theo trạng thái
        groups = {
            "success": success_df,
            "fraud": fraud_df,
            "errors": error_df,
            "invalid": invalid_df
        }

        for group_name, df in groups.items():
            if df.isEmpty():
                continue

            output_path = (
                f"{HDFS_BASE_PATH}/processed_v2/"
                f"{group_name}/batch_id={batch_id:010d}"
            )

            (
                df.write
                .mode("overwrite")
                .option("header", "true")
                .csv(output_path)
            )

            print(f"Saved {group_name}: {output_path}")

        print(f"Batch {batch_id} completed")

    finally:
        batch_df.unpersist()



query = (
    finalized.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option("checkpointLocation", HDFS_CHECKPOINT_PATH)
    .start()
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
finally:
    spark.stop()


try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
finally:
    spark.stop()
