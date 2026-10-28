
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CreditCardKafkaTest")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

transactions = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "credit-card-transactions")
    .option("startingOffsets", "earliest")
    .load()
)

result = transactions.selectExpr(
    "CAST(value AS STRING) AS transaction_json"
)

query = (
    result.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
finally:
    spark.stop()
