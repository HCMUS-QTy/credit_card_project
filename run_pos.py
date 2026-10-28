"""Run paced POS simulation and the Kafka outbox dispatcher together (WSL/Linux).

    python run_pos.py
    python run_pos.py --limit 3

Ctrl+C stops both children. After CSV EOF (or --limit), wait until the outbox
is drained, then exit. Kafka may be started later: dispatcher uses --follow.
"""

import argparse
import math
import signal
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "producer"))


def start_child(script: str, *args: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-u", str(ROOT / script), *args],
        cwd=ROOT,
        # Parent handles terminal Ctrl+C and forwards it to both children.
        start_new_session=True,
    )


def stop_children(children: list[subprocess.Popen]) -> None:
    running = [child for child in children if child.poll() is None]
    for child in running:
        try:
            child.send_signal(signal.SIGINT)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 10
    for child in running:
        try:
            child.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            child.terminate()
            try:
                child.wait(timeout=3)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


def run(limit: int | None = None, interval: float = 3.0) -> int:
    from pos_simulator import open_database

    children = []
    try:
        # Set up WAL/schema before the children open SQLite concurrently.
        with closing(open_database()) as db:
            dispatcher = start_child("producer/kafka_dispatcher.py", "--follow")
            children.append(dispatcher)
            pos_args = ["--interval", str(interval)]
            if limit is not None:
                pos_args.extend(["--limit", str(limit)])
            pos = start_child("producer/pos_simulator.py", *pos_args)
            children.append(pos)
            print("POS + dispatcher started. Press Ctrl+C to stop both.",
                  flush=True)

            waiting_to_drain = False
            while True:
                if dispatcher.poll() is not None:
                    print("ERROR: dispatcher exited unexpectedly; stopping POS.",
                          file=sys.stderr, flush=True)
                    return 1
                pos_result = pos.poll()
                if pos_result is not None:
                    if pos_result != 0:
                        print("ERROR: POS exited with an error; stopping dispatcher.",
                              file=sys.stderr, flush=True)
                        return 1
                    if not waiting_to_drain:
                        print("POS finished. Waiting for remaining PENDING events; "
                              "Ctrl+C to leave them for the next run.", flush=True)
                        waiting_to_drain = True
                    pending = db.execute(
                        "SELECT 1 FROM outbox WHERE status = 'PENDING' LIMIT 1"
                    ).fetchone()
                    if pending is None:
                        print("Outbox drained. All events have Kafka ACKs.", flush=True)
                        return 0
                time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping POS and dispatcher. Pending events remain in SQLite.",
              flush=True)
        return 130
    finally:
        # Complete cleanup even if Ctrl+C is pressed again during shutdown.
        old_int = signal.signal(signal.SIGINT, signal.SIG_IGN)
        old_term = signal.signal(signal.SIGTERM, signal.SIG_IGN)
        old_hup = signal.signal(signal.SIGHUP, signal.SIG_IGN)
        try:
            stop_children(children)
        finally:
            signal.signal(signal.SIGINT, old_int)
            signal.signal(signal.SIGTERM, old_term)
            signal.signal(signal.SIGHUP, old_hup)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximum NEW POS swipes; default: all remaining CSV rows. "
                             "Dispatcher also drains existing PENDING events.")
    parser.add_argument("--interval", type=float, default=3.0,
                        help="Seconds between POS swipes (default: 3)")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be >= 0")
    if not math.isfinite(args.interval) or args.interval < 0:
        parser.error("--interval must be finite and >= 0")

    def request_stop(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGHUP, request_stop)
    return run(args.limit, args.interval)


if __name__ == "__main__":
    sys.exit(main())
