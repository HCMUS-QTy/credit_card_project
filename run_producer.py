import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
env = os.environ.copy()

commands = [
    [
        sys.executable,
        "-u",
        str(ROOT / "producer" / "pos_simulator.py"),
    ],
    [
        sys.executable,
        "-u",
        str(ROOT / "producer" / "kafka_dispatcher.py"),
        "--follow",
    ],
]

processes = []

try:
    for cmd in commands:
        processes.append(
            subprocess.Popen(
                cmd,
                cwd=ROOT,
                env=env,
                start_new_session=True,
            )
        )

    pos, dispatcher = processes
    print("POS + dispatcher started. Ctrl+C de dung ca hai.", flush=True)
    pos_finished = False

    while True:
        if dispatcher.poll() is not None:
            raise RuntimeError("Dispatcher da dung; dung ca POS.")

        pos_code = pos.poll()
        if pos_code is not None:
            if pos_code != 0:
                raise RuntimeError("POS bi loi; dung ca dispatcher.")

            if not pos_finished:
                print(
                    "POS da doc xong CSV. Dispatcher van xu ly PENDING. "
                    "Ctrl+C de dung.",
                    flush=True,
                )
                pos_finished = True

        time.sleep(0.5)

except KeyboardInterrupt:
    print("\nDang dung POS va dispatcher...", flush=True)

finally:
    # Cho phep hoan tat viec dung hai tien trinh.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    for process in processes:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)

    for process in processes:
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()