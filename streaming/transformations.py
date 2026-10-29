"""Đọc, chuẩn hóa, kiểm tra và phân loại giao dịch từ Kafka."""

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from scripts.build_curated import AUDIT_COLUMNS, CORE_COLUMNS

INPUT_COLUMNS = [
    "User", "Card", "Year", "Month", "Day", "Time", "Amount", "Use Chip",
    "Merchant Name", "Merchant City", "Merchant State", "Zip", "MCC", "Errors?",
    "Is Fraud?", "transaction_id", "pos_event_time", "currency",
]
INPUT_SCHEMA = StructType([
    StructField(name, StringType(), True)
    for name in [*INPUT_COLUMNS, "_corrupt_record"]
])

TIME_PATTERN = r"^([01]?[0-9]|2[0-3]):([0-5][0-9])(?::([0-5][0-9]))?$"
AMOUNT_PATTERN = (
    r"^(?:\$[+-]?|[+-]?\$?)"
    r"(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)"
    r"(?:\.[0-9]{1,2})?$"
)
REQUIRED_FIELDS = {
    "transaction_id": "MISSING_ID",
    "User": "MISSING_USER",
    "Card": "MISSING_CARD",
    "merchant": "MISSING_MERCHANT",
    "city": "MISSING_CITY",
}


def transform_transactions(raw_transactions: DataFrame) -> DataFrame:
    """Luồng nghiệp vụ chính; các bước dưới đây chỉ tạo biểu thức Spark."""
    parsed = parse_kafka_messages(raw_transactions)
    normalized = normalize_transaction_fields(parsed)
    validated = add_validation_errors(normalized)
    classified = classify_transactions(validated)
    return classified.select(*CORE_COLUMNS, *AUDIT_COLUMNS)


def parse_kafka_messages(raw_transactions: DataFrame) -> DataFrame:
    payload = F.col("value").cast("string")
    json_options = {
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": "_corrupt_record",
    }
    parsed = raw_transactions.select(
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        payload.alias("raw_value"),
        F.from_json(payload, INPUT_SCHEMA, json_options).alias("transaction"),
    )
    return parsed.select(
        "topic", "partition", "offset", "kafka_timestamp", "raw_value", "transaction.*",
    )


def normalize_transaction_fields(transactions: DataFrame) -> DataFrame:
    amount_text = F.trim(F.col("Amount"))
    transactions = transactions.withColumn(
        "_amount_clean", F.regexp_replace(amount_text, r"[$,]", ""),
    )
    return transactions.withColumns({
        "amount_numeric": F.when(
            amount_text.rlike(AMOUNT_PATTERN),
            F.expr("try_cast(_amount_clean as decimal(18,2))"),
        ),
        "date_formatted": _formatted_date(),
        "time_formatted": _formatted_time(),
        "transaction_id": F.trim("transaction_id"),
        "currency": F.upper(F.trim("currency")),
        "merchant": F.trim("Merchant Name"),
        "city": F.trim("Merchant City"),
        "errors": F.col("Errors?"),
        "is_fraud": F.col("Is Fraud?"),
    })


def _formatted_date() -> Column:
    year = F.trim(F.col("Year"))
    month = F.trim(F.col("Month"))
    day = F.trim(F.col("Day"))
    valid_shape = (
        year.rlike(r"^[0-9]{4}$")
        & (year != "0000")
        & month.rlike(r"^[0-9]{1,2}$")
        & day.rlike(r"^[0-9]{1,2}$")
    )
    # Chỉ padding khi độ dài hợp lệ, tránh cắt tháng 123 thành 12.
    date_text = F.when(
        valid_shape,
        F.concat_ws("-", year, F.lpad(month, 2, "0"), F.lpad(day, 2, "0")),
    )
    parsed_date = F.try_to_timestamp(date_text, F.lit("yyyy-MM-dd"))
    return F.date_format(parsed_date, "dd/MM/yyyy")


def _formatted_time() -> Column:
    time_text = F.trim(F.col("Time"))
    hours = F.regexp_extract(time_text, TIME_PATTERN, 1)
    minutes = F.regexp_extract(time_text, TIME_PATTERN, 2)
    seconds = F.regexp_extract(time_text, TIME_PATTERN, 3)
    seconds = F.when(seconds == "", "00").otherwise(seconds)
    formatted = F.concat_ws(":", F.lpad(hours, 2, "0"), minutes, seconds)
    return F.when(time_text.rlike(TIME_PATTERN), formatted)


def _is_malformed_json() -> Column:
    return (
        F.col("raw_value").isNull()
        | F.col("_corrupt_record").isNotNull()
        | ~F.trim(F.col("raw_value")).rlike(r"^\{")
    )


def add_validation_errors(transactions: DataFrame) -> DataFrame:
    checks = [(_is_malformed_json(), "MALFORMED_JSON")]
    for field, error_code in REQUIRED_FIELDS.items():
        missing_value = F.col(field).isNull() | (F.trim(F.col(field)) == "")
        checks.append((missing_value, error_code))

    fraud_flag = F.upper(F.trim(F.col("is_fraud")))
    valid_fraud_flag = F.coalesce(fraud_flag.isin("YES", "NO"), F.lit(False))
    unsupported_currency = F.col("currency").isNull() | (F.col("currency") != "USD")
    checks.extend([
        (unsupported_currency, "UNSUPPORTED_CURRENCY"),
        (F.col("date_formatted").isNull(), "INVALID_DATE"),
        (F.col("time_formatted").isNull(), "INVALID_TIME"),
        (F.col("amount_numeric").isNull(), "INVALID_AMOUNT"),
        (~valid_fraud_flag, "INVALID_FRAUD_FLAG"),
    ])
    error_messages = [
        F.when(invalid_condition, F.lit(error_code))
        for invalid_condition, error_code in checks
    ]
    return transactions.withColumn("validation_errors", F.concat_ws("|", *error_messages))


def classify_transactions(transactions: DataFrame) -> DataFrame:
    fraud_flag = F.upper(F.trim(F.col("is_fraud")))
    no_error = F.col("errors").isNull() | (F.trim("errors") == "")
    transactions = transactions.withColumns({
        "transaction_status": (
            F.when(fraud_flag == "YES", "FRAUD")
            .when(fraud_flag == "NO", "NON_FRAUD")
            .otherwise("INVALID_FLAG")
        ),
        "error_status": F.when(no_error, "NO_ERROR").otherwise("HAS_ERROR"),
    })
    # Thứ tự ưu tiên giữ nguyên: JSON lỗi → fraud → cờ lỗi → dữ liệu lỗi → lỗi giao dịch.
    final_status = (
        F.when(_is_malformed_json(), "INVALID_DATA")
        .when(fraud_flag == "YES", "REJECTED_FRAUD")
        .when(F.col("transaction_status") == "INVALID_FLAG", "INVALID_FLAG")
        .when(F.col("validation_errors") != "", "INVALID_DATA")
        .when(F.col("error_status") == "HAS_ERROR", "REJECTED_ERROR")
        .otherwise("SUCCESS")
    )
    return transactions.withColumn("final_status", final_status)
