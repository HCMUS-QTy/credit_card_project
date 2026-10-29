"""Khởi chạy Kafka → Spark → Hadoop. Dùng run_streaming.py ở thư mục gốc."""

import fcntl
import hashlib
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

# spark-submit chạy file trực tiếp, cần thư mục gốc để import package.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_curated import HadoopStore
from streaming.batch_processor import BatchProcessor
from streaming.batch_storage import bind_lineage
from streaming.fx_rates import FxRateProvider
from streaming.transformations import transform_transactions


@dataclass(frozen=True)
class StreamingConfig:
    output_path: str
    checkpoint_path: str
    rate_file: Path
    rate_type: str
    spark_master: str
    kafka_servers: str
    kafka_topic: str


def load_config() -> StreamingConfig:
    load_dotenv(PROJECT_ROOT / ".env")
    output_path = os.environ["HDFS_BASE_PATH"].rstrip("/")
    checkpoint_path = os.environ["HDFS_CHECKPOINT_PATH"].rstrip("/")
    if not output_path or not checkpoint_path:
        raise RuntimeError("Can HDFS_BASE_PATH va HDFS_CHECKPOINT_PATH")

    base_currency = os.getenv("FX_BASE_CURRENCY", "USD")
    quote_currency = os.getenv("FX_QUOTE_CURRENCY", "VND")
    if (base_currency, quote_currency) != ("USD", "VND"):
        raise RuntimeError("Pipeline nay chi ho tro USD/VND")
    rate_type = os.getenv("FX_RATE_TYPE", "transfer_buy")
    if rate_type not in {"transfer_buy", "cash_buy", "sell"}:
        raise RuntimeError("FX_RATE_TYPE khong hop le")

    rate_file = Path(os.environ["FX_RATE_FILE"]).expanduser()
    if not rate_file.is_absolute():
        rate_file = PROJECT_ROOT / rate_file
    return StreamingConfig(
        output_path=output_path,
        checkpoint_path=checkpoint_path,
        rate_file=rate_file,
        rate_type=rate_type,
        spark_master=os.getenv("SPARK_MASTER", "local[2]"),
        kafka_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        kafka_topic=os.environ["KAFKA_TOPIC"],
    )


@contextmanager
def single_streaming_process(output_path: str):
    lock_id = hashlib.sha256(output_path.encode()).hexdigest()[:16]
    lock_path = PROJECT_ROOT / "data" / f"streaming_{lock_id}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Mot streaming khac dang dung cung vung du lieu") from None
        yield


def create_spark_session(master: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("CreditCardParse")
        .master(master)
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def require_hdfs(spark: SparkSession, config: StreamingConfig) -> HadoopStore:
    output_store = HadoopStore(spark, config.output_path)
    checkpoint_store = HadoopStore(spark, config.checkpoint_path)
    output_uri = output_store.fs.getUri()
    checkpoint_uri = checkpoint_store.fs.getUri()
    if output_uri.getScheme() != "hdfs" or output_uri.toString() != checkpoint_uri.toString():
        raise RuntimeError("Output/checkpoint phai o cung HDFS. Kiem tra HADOOP_CONF_DIR.")
    return output_store


def read_kafka(spark: SparkSession, config: StreamingConfig) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )


def run_stream(spark: SparkSession, config: StreamingConfig) -> None:
    store = require_hdfs(spark, config)
    lineage_id = bind_lineage(store, config.output_path, config.checkpoint_path)
    rate_provider = FxRateProvider(config.rate_file, config.rate_type)
    processor = BatchProcessor(spark, config.output_path, rate_provider, lineage_id)

    raw_transactions = read_kafka(spark, config)
    transactions = transform_transactions(raw_transactions)
    query = (
        transactions.writeStream
        .foreachBatch(processor.process_batch)
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint_path)
        .start()
    )
    try:
        query.awaitTermination()
    finally:
        query.stop()


def main() -> None:
    config = load_config()
    with single_streaming_process(config.output_path):
        spark = create_spark_session(config.spark_master)
        try:
            run_stream(spark, config)
        except KeyboardInterrupt:
            print("\nDang dung streaming...", flush=True)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
