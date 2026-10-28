
import csv
import json
import time
import random
import argparse
from pathlib import Path
from kafka import KafkaProducer
import os
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")

TOPIC = os.environ["KAFKA_TOPIC"]
CSV_FILE = PROJECT_ROOT / os.environ["CSV_FILE"]

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

DELAY_MIN = float(
    os.getenv("PRODUCER_DELAY_MIN_SECONDS", "1")
)

DELAY_MAX = float(
    os.getenv("PRODUCER_DELAY_MAX_SECONDS", "5")
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=3
    )

    count = 0

    try:
        with open(CSV_FILE, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                # Mô phỏng giao dịch phát sinh sau 1–5 giây
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

                # Gửi dữ liệu và chờ Kafka xác nhận
                result = producer.send(TOPIC, value=row)
                metadata = result.get(timeout=20)

                count += 1
                print(
                    f"[{count}] Offset={metadata.offset} "
                    f"| Amount={row['Amount']} "
                    f"| Fraud={row['Is Fraud?']}",
                    flush=True
                )

                if args.limit > 0 and count >= args.limit:
                    break

    except KeyboardInterrupt:
        print("\nĐã dừng gửi giao dịch.")

    finally:
        producer.flush()
        producer.close()
        print(f"Tổng số giao dịch đã gửi: {count}")


if __name__ == "__main__":
    main()
