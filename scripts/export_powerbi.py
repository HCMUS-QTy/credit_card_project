"""Export one validated curated snapshot for Power BI, without starting Spark."""

import argparse
import csv
import fcntl
import hashlib
import io
import json
import os
import posixpath
import re
import shutil
import subprocess
import tempfile
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "powerbi" / "data"
EXPORT_COLUMNS = [
    "source_snapshot_id", "transaction_id", "User", "Card",
    "date_formatted", "time_formatted", "merchant", "city", "final_status",
    "currency", "amount_numeric", "amount_vnd", "fx_rate",
]
STATUSES = ("SUCCESS", "REJECTED_FRAUD", "REJECTED_ERROR", "INVALID_FLAG", "INVALID_DATA")
DECIMAL_PLACES = {"amount_numeric": 2, "amount_vnd": 2, "fx_rate": 4}
# Power Query Currency.Type is a signed 64-bit integer scaled by 10,000.
POWERBI_DECIMAL_LIMIT = Decimal("922337203685477.5807")


class HdfsStore:
    """Small read-only HDFS CLI adapter; no Java/Spark session is kept alive."""

    def __init__(self, base):
        self.command = shutil.which("hdfs")
        if not self.command:
            raise RuntimeError("Khong tim thay hdfs. Hay chay trong moi truong user hdoop.")
        self.environment = os.environ.copy()
        self.environment.setdefault("HADOOP_HEAPSIZE_MAX", "128m")
        parsed = urlsplit(base)
        filesystem = base if parsed.scheme else self._run("getconf", "-confKey", "fs.defaultFS").strip()
        filesystem = urlsplit(filesystem)
        if filesystem.scheme != "hdfs" or not filesystem.netloc:
            raise RuntimeError("Nguon xuat phai la HDFS. Kiem tra cau hinh Hadoop.")
        self.filesystem = (filesystem.scheme, filesystem.netloc)

    def _run(self, *arguments):
        result = subprocess.run(
            [self.command, *arguments], env=self.environment, check=True,
            capture_output=True, text=True, encoding="utf-8", timeout=120,
        )
        return result.stdout

    def qualified(self, path):
        parsed = urlsplit(str(path))
        if parsed.scheme and (parsed.scheme, parsed.netloc) != self.filesystem:
            raise RuntimeError("Duong dan snapshot khong thuoc HDFS da chon")
        if not parsed.path.startswith("/"):
            raise RuntimeError("Can duong dan HDFS tuyet doi")
        return urlunsplit((*self.filesystem, posixpath.normpath(parsed.path), "", ""))

    def read_text(self, path):
        return self._run("dfs", "-cat", self.qualified(path))

    def read_json(self, path):
        return json.loads(self.read_text(path))

    def exists(self, path):
        result = subprocess.run(
            [self.command, "dfs", "-test", "-e", self.qualified(path)],
            env=self.environment, capture_output=True, text=True, timeout=120,
        )
        if result.returncode not in (0, 1):
            raise RuntimeError(result.stderr.strip() or "Khong kiem tra duoc HDFS")
        return result.returncode == 0

    def csv_parts(self, directory):
        listing = self._run("dfs", "-ls", self.qualified(directory))
        paths = []
        for line in listing.splitlines():
            if not line.startswith("-"):
                continue
            path = line.split(maxsplit=7)[-1]
            filename = posixpath.basename(path)
            if filename.startswith("part-") and filename.endswith(".csv"):
                paths.append(self.qualified(path))
        if not paths:
            raise RuntimeError("Snapshot khong co part-*.csv")
        return sorted(paths)


def select_snapshot(store, base):
    """Freeze CURRENT once, then read only that immutable snapshot."""
    source = store.read_json(f"{base}/curated/CURRENT.json")
    snapshot_id = source.get("snapshot_id")
    if source.get("format_version") != 1 or not isinstance(snapshot_id, str):
        raise RuntimeError("Manifest curated khong dung version 1")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", snapshot_id):
        raise RuntimeError("Snapshot ID khong hop le")
    snapshot = f"{base}/curated/snapshots/{snapshot_id}"
    if (source.get("snapshot_path") != store.qualified(snapshot)
            or source.get("transactions_path") != store.qualified(f"{snapshot}/transactions")):
        raise RuntimeError("Duong dan snapshot khong khop CURRENT")
    if source != store.read_json(f"{snapshot}/manifest.json"):
        raise RuntimeError("CURRENT khong khop manifest cua snapshot")
    if not store.exists(f"{snapshot}/transactions/_SUCCESS"):
        raise RuntimeError("Snapshot curated chua hoan tat")
    return source


def read_transactions(store, source):
    required = set(EXPORT_COLUMNS) - {"source_snapshot_id"}
    for path in store.csv_parts(source["transactions_path"]):
        content = io.StringIO(store.read_text(path), newline="")
        header = next(csv.reader(content), [])
        content.seek(0)
        # New snapshots use standard CSV escaping; legacy Spark used backslashes.
        escape = None if "raw_value" in header else "\\"
        reader = csv.DictReader(content, escapechar=escape, strict=True)
        if len(header) != len(set(header)) or not required.issubset(header):
            raise RuntimeError(f"Schema curated thieu cot hoac trung header: {path}")
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise RuntimeError(f"CSV thieu/thua truong: {path}")
            yield row


def parse_decimal(value, column):
    if value is None or not value.strip():
        return None
    try:
        number = Decimal(value)
        quantum = Decimal(1).scaleb(-DECIMAL_PLACES[column])
        if not number.is_finite() or abs(number) > POWERBI_DECIMAL_LIMIT:
            raise ValueError("vuot kieu Fixed decimal cua Power BI")
        if number != number.quantize(quantum):
            raise ValueError("sai so chu so thap phan")
        return number
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(f"{column} khong hop le: {value}") from exc


def normalize_row(row, snapshot_id):
    transaction_id = row["transaction_id"]
    status = row["final_status"]
    if not transaction_id.strip() or status not in STATUSES:
        raise RuntimeError("Giao dich thieu ID hoac co trang thai la")
    numbers = {column: parse_decimal(row[column], column) for column in DECIMAL_PLACES}
    if status == "SUCCESS":
        if row["currency"] != "USD" or any(value is None for value in numbers.values()):
            raise RuntimeError("SUCCESS thieu so tien/ty gia USD-VND")
        if numbers["fx_rate"] <= 0:
            raise RuntimeError("Ty gia SUCCESS phai lon hon 0")
        expected_vnd = (numbers["amount_numeric"] * numbers["fx_rate"]).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP,
        )
        if numbers["amount_vnd"] != expected_vnd:
            raise RuntimeError("SUCCESS co so tien VND khong khop ty gia da luu")
    exported = {name: row.get(name, "") for name in EXPORT_COLUMNS}
    exported["source_snapshot_id"] = snapshot_id
    for column, number in numbers.items():
        exported[column] = "" if number is None else format(number, f".{DECIMAL_PLACES[column]}f")
    return exported, numbers


def write_validated_csv(store, source, destination):
    stats = {
        "transaction_count": 0,
        "status_counts": dict.fromkeys(STATUSES, 0),
        "success_amount_usd": Decimal("0.00"),
        "success_amount_vnd": Decimal("0.00"),
    }
    ids = set()
    with localcontext() as context, destination.open("w", encoding="utf-8", newline="") as output:
        context.prec = 50
        writer = csv.DictWriter(output, fieldnames=EXPORT_COLUMNS, lineterminator="\n")
        writer.writeheader()
        for row in read_transactions(store, source):
            exported, numbers = normalize_row(row, source["snapshot_id"])
            transaction_id = exported["transaction_id"]
            if transaction_id in ids:
                raise RuntimeError("Curated co transaction_id trung")
            ids.add(transaction_id)
            status = exported["final_status"]
            stats["transaction_count"] += 1
            stats["status_counts"][status] += 1
            if status == "SUCCESS":
                stats["success_amount_usd"] += numbers["amount_numeric"]
                stats["success_amount_vnd"] += numbers["amount_vnd"]
            writer.writerow(exported)
        validate_totals(stats, source)
        output.flush()
        os.fsync(output.fileno())
    for column in ("success_amount_usd", "success_amount_vnd"):
        stats[column] = format(stats[column], ".2f")
    return stats


def validate_totals(stats, source):
    if stats["transaction_count"] != source["curated_transactions"]:
        raise RuntimeError("So giao dich khong khop manifest curated")
    source_statuses = source["status_counts"]
    if set(source_statuses) - set(STATUSES):
        raise RuntimeError("Manifest co trang thai chua ho tro")
    expected = {status: source_statuses.get(status, 0) for status in STATUSES}
    if stats["status_counts"] != expected:
        raise RuntimeError("So luong theo trang thai khong khop manifest")
    for column in ("success_amount_usd", "success_amount_vnd"):
        if stats[column] != Decimal(source[column]):
            raise RuntimeError(f"Tong {column} khong khop manifest")
        if abs(stats[column]) > POWERBI_DECIMAL_LIMIT:
            raise RuntimeError(f"Tong {column} vuot kieu Fixed decimal cua Power BI")
    accounted_rows = source["curated_transactions"] + source["missing_id_rows"] + source["duplicate_extra_rows"]
    if source["source_rows"] != accounted_rows:
        raise RuntimeError("So dong nguon khong khop so dong loai/trung/curated")


def write_json(path, record):
    with path.open("w", encoding="utf-8") as output:
        json.dump(record, output, ensure_ascii=False, indent=2, allow_nan=False)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


@contextmanager
def export_lock(output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / ".export.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Mot export Power BI khac dang chay") from None
        yield


def export_powerbi(store, base, output_dir):
    output_dir = Path(output_dir).resolve()
    with export_lock(output_dir):
        source = select_snapshot(store, base.rstrip("/"))
        export_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ_") + uuid.uuid4().hex[:12]
        staging_parent = output_dir / "_staging"
        staging_parent.mkdir(exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=export_id + "_", dir=staging_parent))
        pointer_temp = output_dir / f".CURRENT_{export_id}.tmp"
        try:
            csv_path = staging / "transactions.csv"
            stats = write_validated_csv(store, source, csv_path)
            with csv_path.open("rb") as csv_bytes:
                digest = hashlib.file_digest(csv_bytes, "sha256").hexdigest()
            manifest = {
                "format_version": 1,
                "export_id": export_id,
                "exported_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_snapshot_id": source["snapshot_id"],
                "source_committed_batch": source["source_committed_batch"],
                "source_transactions_path": source["transactions_path"],
                "source_rows": source["source_rows"],
                "missing_id_rows": source["missing_id_rows"],
                "duplicate_extra_rows": source["duplicate_extra_rows"],
                **stats,
                "csv": {
                    "path": f"snapshots/{export_id}/transactions.csv",
                    "encoding": "utf-8", "delimiter": ",",
                    "columns": EXPORT_COLUMNS, "sha256": digest,
                },
                "manifest_path": f"snapshots/{export_id}/manifest.json",
            }
            write_json(staging / "manifest.json", manifest)
            snapshots = output_dir / "snapshots"
            snapshots.mkdir(exist_ok=True)
            staging.rename(snapshots / export_id)
            write_json(pointer_temp, manifest)
            # Only this final namespace operation makes the new bundle visible.
            os.replace(pointer_temp, output_dir / "CURRENT.json")
            return manifest
        finally:
            if staging.exists():
                shutil.rmtree(staging)
            pointer_temp.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="HDFS base; mac dinh HDFS_BASE_PATH trong .env")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    arguments = parser.parse_args()
    load_dotenv(ROOT / ".env")
    base = (arguments.base or os.getenv("HDFS_BASE_PATH", "")).rstrip("/")
    if not base:
        parser.error("Can --base hoac HDFS_BASE_PATH")
    try:
        manifest = export_powerbi(HdfsStore(base), base, arguments.output_dir)
    except (OSError, ValueError, KeyError, csv.Error, RuntimeError, subprocess.SubprocessError) as exc:
        raise SystemExit(f"EXPORT FAILED: {exc}") from exc
    output_dir = arguments.output_dir.resolve()
    print(f"POWERBI_CURRENT: {output_dir / 'CURRENT.json'}")
    print(f"POWERBI_CSV: {output_dir / manifest['csv']['path']}")
    print(f"SOURCE_SNAPSHOT_ID: {manifest['source_snapshot_id']}")
    print(f"TRANSACTION_COUNT: {manifest['transaction_count']}")
    print(f"SUCCESS_COUNT: {manifest['status_counts']['SUCCESS']}")
    print(f"SUCCESS_AMOUNT_USD: {manifest['success_amount_usd']}")
    print(f"SUCCESS_AMOUNT_VND: {manifest['success_amount_vnd']}")
    if shutil.which("wslpath"):
        result = subprocess.run(["wslpath", "-w", str(output_dir)], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"POWERBI_WINDOWS_FOLDER: {result.stdout.strip()}")


if __name__ == "__main__":
    main()
