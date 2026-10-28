"""Simulate POS swipes from a fixed CSV using a durable SQLite outbox.

Every committed row is one simulated POS event. Restarting resumes from the
committed CSV cursor rather than replaying rows as new swipes.
"""

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def project_path(name: str) -> Path:
    path = Path(os.environ[name]).expanduser()
    return path if path.is_absolute() else ROOT / path


CSV_FILE = project_path("CSV_FILE")
DB_FILE = project_path("OUTBOX_DB_PATH")
SOURCE_FILE = str(CSV_FILE.resolve())


def open_database() -> sqlite3.Connection:
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_FILE, timeout=30, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("""
        CREATE TABLE IF NOT EXISTS outbox (
            transaction_id TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING'
                CHECK (status IN ('PENDING', 'SENT')),
            created_at TEXT NOT NULL,
            sent_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS simulator_state (
            source_file TEXT PRIMARY KEY,
            file_sha256 TEXT NOT NULL,
            next_line INTEGER NOT NULL DEFAULT 2
        )
    """)
    return db


def file_digest(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def initialize_source(db: sqlite3.Connection) -> None:
    digest = file_digest(CSV_FILE)
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute(
            "INSERT OR IGNORE INTO simulator_state "
            "(source_file, file_sha256, next_line) VALUES (?, ?, 2)",
            (SOURCE_FILE, digest),
        )
        saved_digest = db.execute(
            "SELECT file_sha256 FROM simulator_state WHERE source_file = ?",
            (SOURCE_FILE,),
        ).fetchone()[0]
        if saved_digest != digest:
            raise RuntimeError(
                "CSV da thay doi so voi lan chay truoc. "
                "Khong tiep tuc voi cursor cu; hay kiem tra nguon du lieu."
            )
        db.commit()
    except Exception:
        db.rollback()
        raise


def simulate_swipes(db: sqlite3.Connection, limit: int) -> int:
    if limit < 0:
        raise ValueError("--limit phai >= 0")
    created = 0
    with CSV_FILE.open("r", newline="", encoding="utf-8-sig") as stream:
        for line_number, row in enumerate(csv.DictReader(stream), start=2):
            if created >= limit:
                break

            db.execute("BEGIN IMMEDIATE")
            try:
                next_line = db.execute(
                    "SELECT next_line FROM simulator_state WHERE source_file = ?",
                    (SOURCE_FILE,),
                ).fetchone()[0]
                if line_number < next_line:
                    db.rollback()
                    continue
                if line_number != next_line:
                    raise RuntimeError("CSV cursor khong lien tuc")

                tx_id = str(uuid.uuid4())
                event_time = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).isoformat()
                event = dict(row)
                event.update(
                    transaction_id=tx_id,
                    pos_event_time=event_time,
                    currency="USD",
                )
                db.execute(
                    "INSERT INTO outbox "
                    "(transaction_id, payload_json, created_at) VALUES (?, ?, ?)",
                    (tx_id, json.dumps(event, ensure_ascii=False), event_time),
                )
                db.execute(
                    "UPDATE simulator_state SET next_line = ? WHERE source_file = ?",
                    (line_number + 1, SOURCE_FILE),
                )
                db.commit()
            except Exception:
                db.rollback()
                raise

            created += 1
            print(f"POS swipe | CSV line {line_number} | ID {tx_id} | PENDING")
    return created


def print_status(db: sqlite3.Connection) -> None:
    rows = db.execute(
        "SELECT status, COUNT(*) FROM outbox GROUP BY status ORDER BY status"
    ).fetchall()
    counts = dict(rows)
    cursor = db.execute(
        "SELECT next_line FROM simulator_state WHERE source_file = ?",
        (SOURCE_FILE,),
    ).fetchone()
    print(f"OUTBOX_DB: {DB_FILE}")
    print(f"PENDING: {counts.get('PENDING', 0)}")
    print(f"SENT: {counts.get('SENT', 0)}")
    print(f"NEXT_CSV_LINE: {cursor[0] if cursor else 2}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    with open_database() as db:
        initialize_source(db)
        if args.status:
            print_status(db)
            return
        count = simulate_swipes(db, args.limit)
        print(f"Da ghi ben vung {count} giao dich gia lap vao SQLite")
        print_status(db)


if __name__ == "__main__":
    main()
