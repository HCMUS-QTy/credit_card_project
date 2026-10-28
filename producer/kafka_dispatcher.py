"""Send durable POS outbox events to Kafka and mark SENT after a broker ACK.

Delivery is at-least-once: a crash after Kafka ACK but before the SQLite
commit can resend an event with the SAME transaction_id. Downstream systems
must deduplicate by that business ID before computing totals.

Run from the project root:
    python producer/kafka_dispatcher.py --limit 3
    python producer/kafka_dispatcher.py --follow
    python producer/kafka_dispatcher.py --status
"""

import argparse
import fcntl
import os
import random
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from kafka import KafkaProducer

from pos_simulator import DB_FILE, open_database, print_status

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

TOPIC = os.environ["KAFKA_TOPIC"]
BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]


def open_producer():
    options = {
        "bootstrap_servers": BOOTSTRAP_SERVERS.split(","),
        "acks": "all",
        "retries": 5,
        "retry_backoff_ms": 1000,
        "request_timeout_ms": 20000,
        "max_in_flight_requests_per_connection": 1,
    }
    # kafka-python releases differ in producer-idempotence support.
    # The outbox + stable transaction_id works even without this option.
    if "enable_idempotence" in KafkaProducer.DEFAULT_CONFIG:
        options["enable_idempotence"] = True
    else:
        print("NOTE: Kafka client has no enable_idempotence option; "
              "downstream dedup by transaction_id is required", flush=True)
    return KafkaProducer(**options)


def next_pending(db: sqlite3.Connection):
    return db.execute(
        "SELECT transaction_id, payload_json FROM outbox "
        "WHERE status = 'PENDING' ORDER BY rowid LIMIT 1"
    ).fetchone()


def mark_attempt(db: sqlite3.Connection, transaction_id: str):
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute(
            "UPDATE outbox SET attempts = attempts + 1 "
            "WHERE transaction_id = ? AND status = 'PENDING'",
            (transaction_id,),
        )
        db.commit()
    except Exception:
        db.rollback()
        raise


def mark_sent(db: sqlite3.Connection, transaction_id: str):
    sent_at = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).isoformat()
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute(
            "UPDATE outbox SET status = 'SENT', sent_at = ? "
            "WHERE transaction_id = ? AND status = 'PENDING'",
            (sent_at, transaction_id),
        )
        db.commit()
    except Exception:
        db.rollback()
        raise


def dispatch(db: sqlite3.Connection, producer, limit: int) -> int:
    count = 0
    while count < limit:
        row = next_pending(db)
        if row is None:
            break
        transaction_id, payload_json = row

        delay = random.uniform(1.0, 5.0)
        print(f"WAIT | {delay:.2f}s | transaction_id={transaction_id}",
              flush=True)
        time.sleep(delay)

        # A crash after this commit but before/after send keeps the event
        # PENDING, so a restart will send the SAME ID again.
        mark_attempt(db, transaction_id)
        try:
            metadata = producer.send(
                TOPIC,
                key=transaction_id.encode("utf-8"),
                value=payload_json.encode("utf-8"),
            ).get(timeout=30)
        except Exception as exc:
            print(f"SEND FAILED | {transaction_id} | {type(exc).__name__}: {exc}",
                  flush=True)
            print("SQLite row is still PENDING. Restart to retry SAME ID.",
                  flush=True)
            break

        # This is deliberately AFTER the Kafka ACK. There is an unavoidable
        # crash window between ACK and local commit (at-least-once delivery).
        mark_sent(db, transaction_id)
        count += 1
        print(f"SENT | {transaction_id} | topic={metadata.topic} "
              f"partition={metadata.partition} offset={metadata.offset}",
              flush=True)
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=3,
                        help="Maximum number of pending events to attempt")
    parser.add_argument("--status", action="store_true",
                        help="Show outbox status without sending")
    parser.add_argument("--follow", action="store_true",
                        help="Keep watching for new pending events")
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit must be >= 0")

    with open_database() as db:
        if args.status:
            print_status(db)
            return

        # Keep one dispatcher process per outbox database, including across
        # restarts. POS simulator may still insert events concurrently.
        lock_path = DB_FILE.with_name(DB_FILE.name + ".dispatch.lock")
        with lock_path.open("w") as lock_file:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("Another Kafka dispatcher is already running")

            if next_pending(db) is None and not args.follow:
                print("No PENDING events")
                print_status(db)
                return

            producer = None
            try:
                if args.follow:
                    print("Watching outbox continuously. Press Ctrl+C to stop.",
                          flush=True)
                    while True:
                        if producer is None:
                            try:
                                producer = open_producer()
                            except Exception as exc:
                                print(f"Kafka unavailable: {type(exc).__name__}: {exc}",
                                      flush=True)
                                print("Will retry connection; SQLite events remain PENDING.",
                                      flush=True)
                                time.sleep(5)
                                continue
                        processed = dispatch(db, producer, 100)
                        if processed == 0:
                            time.sleep(2)
                else:
                    producer = open_producer()
                    sent_count = dispatch(db, producer, args.limit)
                    print(f"Sent in this run: {sent_count}")
            except KeyboardInterrupt:
                print("\nDispatcher stopped. PENDING rows are preserved.")
            finally:
                if producer is not None:
                    producer.close(timeout=5)
                print_status(db)


if __name__ == "__main__":
    main()
