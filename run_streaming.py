
import os
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv
import shutil

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

master = os.environ["SPARK_MASTER"]
package = os.environ["SPARK_KAFKA_PACKAGE"]

env = os.environ.copy()
env["PYSPARK_PYTHON"] = sys.executable
env["PYSPARK_DRIVER_PYTHON"] = sys.executable


hdfs_cmd = shutil.which("hdfs")

if not env.get("HADOOP_CONF_DIR"):
    if not hdfs_cmd:
        raise RuntimeError("Khong tim thay lenh hdfs")

    conf = (
        Path(hdfs_cmd).resolve().parent.parent
        / "etc"
        / "hadoop"
    )

    if not (
        (conf / "core-site.xml").is_file()
        and (conf / "hdfs-site.xml").is_file()
    ):
        raise RuntimeError(
            f"Khong tim thay Hadoop config tai {conf}"
        )

    env["HADOOP_CONF_DIR"] = str(conf)


cmd = [
    "spark-submit",
    "--master", master,
    "--packages", package,
    str(ROOT / "streaming" / "parse_transactions.py"),
]

print("Starting Spark Streaming...", flush=True)

subprocess.run(
    cmd,
    cwd=ROOT,
    env=env,
    check=True
)
