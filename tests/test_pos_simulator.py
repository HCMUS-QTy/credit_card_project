"""POS checks using temporary CSV/SQLite files; no Kafka or real outbox."""

import contextlib
import csv
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from producer import pos_simulator as pos


class PosSimulatorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="credit-card-pos-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "transactions.csv"
        self.database = self.root / "outbox.sqlite3"
        self.row = dict(zip(pos.EXPECTED_COLUMNS, [
            "0", "0", "2020", "1", "1", "12:00", "$-2.00", "Swipe",
            "Cửa hàng, A", "City", "", "00123", "1", "", "Yes",
        ]))
        self.write_source([self.row] * 3)
        paths = patch.multiple(pos, CSV_FILE=self.source, DB_FILE=self.database,
                               SOURCE_FILE=str(self.source.resolve()))
        paths.start()
        self.addCleanup(paths.stop)
        environment = patch.dict(os.environ, {
            "PRODUCER_DELAY_MIN_SECONDS": "1",
            "PRODUCER_DELAY_MAX_SECONDS": "5",
        })
        environment.start()
        self.addCleanup(environment.stop)
        self.db = pos.open_database()
        self.addCleanup(self.db.close)
        pos.initialize_source(self.db)
        self.output = io.StringIO()
        redirected = contextlib.redirect_stdout(self.output)
        redirected.__enter__()
        self.addCleanup(redirected.__exit__, None, None, None)

    def write_source(self, rows):
        with self.source.open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=pos.EXPECTED_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    def stored(self):
        return self.db.execute(
            "SELECT transaction_id, payload_json FROM outbox ORDER BY rowid"
        ).fetchall()

    def next_line(self):
        return self.db.execute("SELECT next_line FROM simulator_state").fetchone()[0]

    def test_limit_resume_and_preserve_raw_payload(self):
        with patch.object(pos.time, "sleep") as sleep:
            self.assertEqual(pos.simulate_swipes(self.db, limit=2), 2)
            original = self.stored()
            self.assertEqual(self.next_line(), 4)
            self.assertEqual(pos.simulate_swipes(self.db, limit=2), 1)
            self.assertEqual(pos.simulate_swipes(self.db), 0)
        self.assertEqual(sleep.call_count, 3)
        self.assertEqual(self.stored()[:2], original)
        self.assertEqual(len({tx_id for tx_id, _ in self.stored()}), 3)
        self.assertEqual(self.next_line(), 5)
        for tx_id, payload in self.stored():
            event = json.loads(payload)
            self.assertEqual({k: event[k] for k in pos.EXPECTED_COLUMNS}, self.row)
            self.assertEqual(event["transaction_id"], tx_id)
            self.assertEqual(event["currency"], "USD")
            self.assertTrue(event["pos_event_time"].endswith("+07:00"))

    def test_wait_precedes_write_transaction(self):
        def waiting(seconds):
            self.assertGreaterEqual(seconds, 1)
            self.assertLessEqual(seconds, 5)
            self.assertFalse(self.db.in_transaction)
            self.assertEqual(self.stored(), [])
            # A second connection can obtain a write lock during the wait.
            with contextlib.closing(sqlite3.connect(self.database, timeout=0)) as other:
                other.execute("BEGIN IMMEDIATE")
                other.rollback()

        with patch.object(pos.time, "sleep", side_effect=waiting):
            self.assertEqual(pos.simulate_swipes(self.db, limit=1), 1)
        self.assertIn("SAVED", self.output.getvalue())

    def test_invalid_delay_does_not_create_events(self):
        for minimum, maximum in [("0", "5"), ("2", "1"), ("1", "6"),
                                 ("NaN", "5"), ("1", "inf"), ("abc", "5")]:
            with self.subTest(minimum=minimum, maximum=maximum):
                with patch.dict(os.environ, {"PRODUCER_DELAY_MIN_SECONDS": minimum,
                                             "PRODUCER_DELAY_MAX_SECONDS": maximum}):
                    with self.assertRaises(ValueError):
                        pos.simulate_swipes(self.db)
        self.assertEqual(self.stored(), [])
        self.assertEqual(self.next_line(), 2)

    def test_negative_limit_is_rejected(self):
        with self.assertRaises(ValueError):
            pos.simulate_swipes(self.db, limit=-1)
        self.assertEqual(self.stored(), [])

    def test_wrong_header_is_rejected(self):
        self.source.write_text("unexpected\nvalue\n", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "Header CSV"):
            pos.simulate_swipes(self.db)
        self.assertEqual(self.stored(), [])
        self.assertEqual(self.next_line(), 2)

    def test_missing_and_extra_fields_are_rejected_without_advancing(self):
        for values in [list(self.row.values())[:-1], list(self.row.values()) + ["extra"]]:
            with self.subTest(field_count=len(values)):
                with self.source.open("w", newline="", encoding="utf-8") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(pos.EXPECTED_COLUMNS)
                    writer.writerow(values)
                with self.assertRaisesRegex(RuntimeError, "thừa trường"):
                    pos.simulate_swipes(self.db)
                self.assertEqual(self.stored(), [])
                self.assertEqual(self.next_line(), 2)

    def test_changed_source_cannot_resume(self):
        self.write_source([self.row])
        with self.assertRaisesRegex(RuntimeError, "CSV da thay doi"):
            pos.initialize_source(self.db)
        self.assertFalse(self.db.in_transaction)
        self.assertEqual(self.stored(), [])

    def test_interrupt_during_wait_creates_no_event(self):
        with patch.object(pos.time, "sleep", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                pos.simulate_swipes(self.db)
        self.assertFalse(self.db.in_transaction)
        self.assertEqual(self.stored(), [])
        self.assertEqual(self.next_line(), 2)

    def test_interrupt_after_insert_rolls_back_event_and_cursor(self):
        connection = unittest.mock.Mock(wraps=self.db)

        def execute(sql, *args):
            if sql.startswith("UPDATE simulator_state"):
                raise KeyboardInterrupt
            return self.db.execute(sql, *args)

        connection.execute.side_effect = execute
        with patch.object(pos.time, "sleep"):
            with self.assertRaises(KeyboardInterrupt):
                pos.simulate_swipes(connection)
            self.assertFalse(self.db.in_transaction)
            self.assertEqual(self.stored(), [])
            self.assertEqual(self.next_line(), 2)
            self.assertEqual(pos.simulate_swipes(self.db, limit=1), 1)

    def test_lock_rejects_second_pos_and_releases_after_error(self):
        with self.assertRaisesRegex(RuntimeError, "simulated error"):
            with pos.simulator_lock():
                with self.assertRaisesRegex(RuntimeError, "Mot POS khac"):
                    with pos.simulator_lock():
                        self.fail("Second POS acquired the lock")
                raise RuntimeError("simulated error")
        with pos.simulator_lock():
            pass

    def test_main_stops_cleanly_closes_database_and_releases_lock(self):
        connection = pos.open_database()
        with patch.object(sys, "argv", ["pos_simulator.py", "--limit", "1"]), \
                patch.object(pos, "open_database", return_value=connection), \
                patch.object(pos.time, "sleep", side_effect=KeyboardInterrupt):
            pos.main()
        self.assertIn("POS da dung", self.output.getvalue())
        self.assertEqual(self.stored(), [])
        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")
        with pos.simulator_lock():
            pass

    def test_status_does_not_initialize_source_or_require_csv(self):
        self.source.unlink()
        self.db.execute("DELETE FROM simulator_state")
        with patch.object(sys, "argv", ["pos_simulator.py", "--status"]):
            pos.main()
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM simulator_state").fetchone()[0], 0)
        self.assertIn("PENDING: 0", self.output.getvalue())


if __name__ == "__main__":
    unittest.main()
