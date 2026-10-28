"""Outbox/resume and subprocess lifecycle checks; no Kafka server required.

Run: python -m unittest discover -s tests -p 'test_pos_runner.py' -v
"""

import csv
import fcntl
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "producer"))
with patch.dict(os.environ, {
    "CSV_FILE": "/tmp/pos-test-unused.csv",
    "OUTBOX_DB_PATH": "/tmp/pos-test-unused.sqlite3",
    "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1",
    "KAFKA_TOPIC": "pos-test",
}):
    import pos_simulator as pos
    import kafka_dispatcher as dispatcher


class PosFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.csv_path = self.folder / "transactions.csv"
        self.db_path = self.folder / "outbox.sqlite3"
        self.write_csv(4)
        config = patch.multiple(pos, CSV_FILE=self.csv_path,
                                DB_FILE=self.db_path, SOURCE_FILE=str(self.csv_path))
        config.start()
        self.addCleanup(config.stop)
        self.db = pos.open_database()
        self.addCleanup(self.db.close)
        pos.initialize_source(self.db)
        self.env = dict(os.environ, CSV_FILE=str(self.csv_path),
                        OUTBOX_DB_PATH=str(self.db_path),
                        KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:1", KAFKA_TOPIC="pos-test")

    def write_csv(self, count):
        with self.csv_path.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["User", "Card", "Amount"])
            writer.writerows([[i, 0, "$10.00"] for i in range(count)])

    def rows(self):
        return self.db.execute(
            "SELECT transaction_id, payload_json, status, attempts FROM outbox ORDER BY rowid"
        ).fetchall()

    def cursor(self):
        return self.db.execute("SELECT next_line FROM simulator_state").fetchone()[0]


class PosTests(PosFixture):
    def test_default_reads_all_and_restart_at_eof_creates_nothing(self):
        with patch.object(pos.time, "sleep") as sleep:
            self.assertEqual(pos.simulate_swipes(self.db), 4)
        self.assertEqual([call.args for call in sleep.call_args_list], [(3.0,)] * 3)
        original = self.rows()
        self.assertEqual(self.cursor(), 6)
        self.assertEqual(pos.simulate_swipes(self.db), 0)
        self.assertEqual(self.rows(), original)

    def test_limit_and_restart_resume_existing_cursor_and_ids(self):
        self.assertEqual(pos.simulate_swipes(self.db, 1, 0), 1)
        original = self.rows()[0]
        self.assertEqual(pos.simulate_swipes(self.db, interval=0), 3)
        self.assertEqual(self.rows()[0], original)
        self.assertEqual(len({r[0] for r in self.rows()}), 4)
        self.assertEqual([json.loads(r[1])["User"] for r in self.rows()],
                         ["0", "1", "2", "3"])

    def test_interrupt_while_sleeping_preserves_last_committed_cursor(self):
        def interrupt(seconds):
            self.assertFalse(self.db.in_transaction)
            raise KeyboardInterrupt

        with patch.object(pos.time, "sleep", side_effect=interrupt):
            with self.assertRaises(KeyboardInterrupt):
                pos.simulate_swipes(self.db)
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.cursor(), 3)
        self.assertEqual(pos.simulate_swipes(self.db, interval=0), 3)

    def test_interrupt_between_insert_and_cursor_update_rolls_back_both(self):
        db = self.db

        class InterruptedConnection:
            def execute(self, sql, *args):
                if sql.startswith("UPDATE simulator_state"):
                    raise KeyboardInterrupt
                return db.execute(sql, *args)

            def rollback(self):
                db.rollback()

        with self.assertRaises(KeyboardInterrupt):
            pos.simulate_swipes(InterruptedConnection(), 1, 0)
        self.assertEqual(self.rows(), [])
        self.assertEqual(self.cursor(), 2)
        self.assertFalse(self.db.in_transaction)
        self.assertEqual(pos.simulate_swipes(self.db, interval=0), 4)

    def test_changed_csv_is_rejected_without_resetting_cursor(self):
        pos.simulate_swipes(self.db, 1, 0)
        original = self.rows()
        self.write_csv(5)
        with self.assertRaisesRegex(RuntimeError, "CSV da thay doi"):
            pos.initialize_source(self.db)
        self.assertEqual(self.rows(), original)
        self.assertEqual(self.cursor(), 3)

    def test_second_simulator_cannot_consume_the_same_outbox(self):
        with self.db_path.with_name(self.db_path.name + ".pos.lock").open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = subprocess.run(
                [sys.executable, str(ROOT / "producer/pos_simulator.py"),
                 "--limit", "1", "--interval", "0"],
                env=self.env, capture_output=True, text=True, timeout=10,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Another POS simulator", result.stderr)
        self.assertEqual(self.rows(), [])

    def test_ack_failure_retains_pending_ids_and_only_ack_marks_sent(self):
        pos.simulate_swipes(self.db, 2, 0)
        original_ids = [r[0] for r in self.rows()]
        producer = Mock()
        producer.send.return_value.get.side_effect = [
            SimpleNamespace(topic="pos-test", partition=0, offset=1),
            RuntimeError("test ACK failure"),
        ]
        def check_sleep(seconds):
            self.assertFalse(self.db.in_transaction)
            self.assertEqual(seconds, 2.0)
        with patch.object(dispatcher.random, "uniform", return_value=2.0), \
                patch.object(dispatcher.time, "sleep", side_effect=check_sleep):
            self.assertEqual(dispatcher.dispatch(self.db, producer, 2), 1)
        self.assertEqual([r[0] for r in self.rows()], original_ids)
        self.assertEqual([r[2] for r in self.rows()], ["SENT", "PENDING"])

    def test_interrupt_before_send_does_not_mark_attempt_or_sent(self):
        pos.simulate_swipes(self.db, 1, 0)
        producer = Mock()
        with patch.object(dispatcher.time, "sleep", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                dispatcher.dispatch(self.db, producer, 1)
        self.assertEqual(self.rows()[0][2:], ("PENDING", 0))
        producer.send.assert_not_called()


class RunnerTests(PosFixture):
    # A fake transport crosses real process boundaries while SQLite and the
    # actual launcher/POS/dispatcher code run normally. No network is used.
    def setUp(self):
        super().setUp()
        self.fake = self.folder / "transport"
        self.fake.mkdir()
        (self.fake / "kafka.py").write_text('''
import os
from pathlib import Path
from types import SimpleNamespace
class KafkaProducer:
    DEFAULT_CONFIG = {"enable_idempotence": False}
    def __init__(self, **options):
        if os.environ.get("TEST_DISPATCH_CRASH"):
            raise SystemExit(7)
        if not Path(os.environ["TEST_BROKER_READY"]).exists():
            raise RuntimeError("test broker is offline")
    def send(self, topic, key, value):
        class Future:
            def get(self, timeout):
                if not Path(os.environ["TEST_BROKER_READY"]).exists():
                    raise RuntimeError("test broker went offline")
                return SimpleNamespace(topic=topic, partition=0, offset=1)
        return Future()
    def close(self, timeout):
        pass
''')
        self.ready = self.folder / "broker_ready"
        self.env.update(TEST_BROKER_READY=str(self.ready),
                        PYTHONPATH=str(self.fake) + os.pathsep +
                        self.env.get("PYTHONPATH", ""))

    def start_runner(self, *args):
        log = self.folder / "runner.log"
        stream = log.open("w")
        process = subprocess.Popen(
            [sys.executable, "-u", str(ROOT / "run_pos.py"), *args],
            cwd=self.folder, env=self.env, stdout=stream, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.addCleanup(stream.close)
        self.addCleanup(self.stop_runner, process)
        return process, log

    @staticmethod
    def stop_runner(process):
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=16)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

    def wait_for(self, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        self.fail("Timed out waiting for runner state")

    def test_one_command_drains_then_stops_and_can_restart(self):
        self.ready.touch()
        process, log = self.start_runner("--limit", "1", "--interval", "0")
        self.assertEqual(process.wait(timeout=12), 0, log.read_text())
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.rows()[0][2], "SENT")
        self.assertIn("Outbox drained", log.read_text())
        original = self.rows()[0]
        process, log = self.start_runner("--limit", "1", "--interval", "0")
        self.assertEqual(process.wait(timeout=12), 0, log.read_text())
        self.assertEqual(self.rows()[0], original)
        self.assertEqual([r[2] for r in self.rows()], ["SENT", "SENT"])
        self.assertEqual(self.cursor(), 4)

    def test_no_limit_runs_until_eof_and_restart_does_not_replay(self):
        self.ready.touch()
        process, log = self.start_runner("--interval", "0")
        self.assertEqual(process.wait(timeout=30), 0, log.read_text())
        original = self.rows()
        self.assertEqual(len(original), 4)
        self.assertEqual([r[2] for r in original], ["SENT"] * 4)
        process, log = self.start_runner("--interval", "0")
        self.assertEqual(process.wait(timeout=10), 0, log.read_text())
        self.assertEqual(self.rows(), original)

    def test_waits_for_offline_broker_then_drains_after_recovery(self):
        process, log = self.start_runner("--limit", "1", "--interval", "0")
        self.wait_for(lambda: "test broker is offline" in log.read_text()
                      and len(self.rows()) == 1)
        original_id = self.rows()[0][0]
        self.assertIsNone(process.poll())
        self.assertEqual(self.rows()[0][2], "PENDING")
        self.ready.touch()
        self.assertEqual(process.wait(timeout=16), 0, log.read_text())
        self.assertEqual(self.rows()[0][0], original_id)
        self.assertEqual(self.rows()[0][2], "SENT")

    def test_ctrl_c_stops_both_children_and_preserves_cursor(self):
        process, log = self.start_runner("--interval", "30")
        self.wait_for(lambda: len(self.rows()) == 1
                      and "test broker is offline" in log.read_text())
        process.send_signal(signal.SIGINT)
        self.assertEqual(process.wait(timeout=16), 130, log.read_text())
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.cursor(), 3)
        self.assertEqual(self.rows()[0][2], "PENDING")
        # Both children hold their lock until exit. Reacquiring both checks
        # cleanup without depending on /proc or a particular PID namespace.
        for suffix in (".pos.lock", ".dispatch.lock"):
            with self.db_path.with_name(self.db_path.name + suffix).open("w") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_dispatcher_crash_stops_pos_and_returns_failure(self):
        self.env["TEST_DISPATCH_CRASH"] = "1"
        process, log = self.start_runner("--interval", "30")
        self.assertEqual(process.wait(timeout=12), 1, log.read_text())
        count = len(self.rows())
        self.assertLessEqual(count, 1)
        self.assertEqual(self.cursor(), count + 2)
        self.assertIn("dispatcher exited unexpectedly", log.read_text())

    def test_pos_failure_stops_dispatcher(self):
        self.ready.touch()
        self.write_csv(5)  # Previously registered source hash no longer matches.
        process, log = self.start_runner("--interval", "0")
        self.assertEqual(process.wait(timeout=12), 1, log.read_text())
        self.assertEqual(self.rows(), [])
        self.assertIn("POS exited with an error", log.read_text())


if __name__ == "__main__":
    unittest.main()
