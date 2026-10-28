"""Integration checks using real local Spark/Hadoop APIs; no Kafka or HDFS needed.

Run from the project root:
    python -m unittest discover -s tests -v
"""

import csv
import json
import os
import sys
import tempfile
import unittest
from collections import Counter
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlparse

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import build_reports as reports
from scripts.build_curated import ALL_COLUMNS


def transaction(tx_id, amount="10.00", status="SUCCESS", merchant="Shop A", date="01/01/2020"):
    row = dict.fromkeys(ALL_COLUMNS, "")
    row.update(transaction_id=tx_id, final_status=status, amount_numeric=amount,
               date_formatted=date, merchant=merchant, currency="USD",
               delivery_count="1", first_seen_batch_id="0")
    if status == "SUCCESS" and amount is not None:
        row.update(fx_rate="25640.0000", amount_vnd=str(Decimal(amount) * Decimal("25640")))
    return row


def path_of(uri):
    return Path(urlparse(uri).path)


def read_csv(uri):
    parts = sorted(path_of(uri).glob("part-*.csv"))
    assert len(parts) == 1, parts
    with parts[0].open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


class BuildReportsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.spark = (SparkSession.builder.master("local[2]").appName("TestCreditCardReports")
                     .config("spark.ui.enabled", "false")
                     .config("spark.ui.showConsoleProgress", "false")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="credit-card-reports-")
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name) / "base"

    def snapshot(self, snapshot_id, rows, missing=0, duplicates=0):
        directory = self.base / "curated/snapshots" / snapshot_id
        data = directory / "transactions"
        data.mkdir(parents=True)
        with (data / "part-00000.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=[*ALL_COLUMNS, "delivery_count", "first_seen_batch_id"])
            writer.writeheader()
            writer.writerows(rows)
        (data / "_SUCCESS").touch()
        success = [r for r in rows if r["final_status"] == "SUCCESS"]
        store = reports.HadoopStore(self.spark, str(self.base))
        manifest = {
            "format_version": 1, "snapshot_id": snapshot_id,
            "source_committed_batch": 116, "source_rows": len(rows) + missing + duplicates,
            "missing_id_rows": missing, "duplicate_extra_rows": duplicates,
            "curated_transactions": len(rows),
            "status_counts": dict(Counter(r["final_status"] for r in rows)),
            "success_amount_usd": str(sum((Decimal(r["amount_numeric"] or "0") for r in success), Decimal("0.00"))),
            "success_amount_vnd": str(sum((Decimal(r["amount_vnd"] or "0") for r in success), Decimal("0.00"))),
            "snapshot_path": store.qualified(str(directory)),
            "transactions_path": store.qualified(str(data)),
        }
        (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        (self.base / "curated/CURRENT.json").write_text(json.dumps(manifest), encoding="utf-8")
        return manifest

    def baseline(self):
        self.snapshot("old_snapshot", [transaction("old", "1000000.00")])
        rows = [transaction(f"tx-{i}", merchant="Shop A" if i % 2 else "Cửa hàng, B",
                            date="31/12/2019" if i % 2 else "01/01/2020") for i in range(48)]
        rows.append(transaction("tx-48", "3385.38", merchant=None))
        rows.extend(transaction(f"error-{i}", "999999.00", "REJECTED_ERROR",
                                date=None if i == 0 else "01/01/2020") for i in range(3))
        return self.snapshot("current_snapshot", rows, missing=26, duplicates=2)

    def test_baseline_totals_tables_and_repeat(self):
        source = self.baseline()
        source_before = (self.base / "curated/CURRENT.json").read_bytes()
        first, summary = reports.build_reports(self.spark, str(self.base), top_n=2)
        self.assertEqual(summary["transaction_count"], 52)
        self.assertEqual(summary["source_rows"], 80)
        self.assertEqual(summary["missing_id_rows"], 26)
        self.assertEqual(summary["duplicate_extra_rows"], 2)
        self.assertEqual(summary["success_count"], 49)
        self.assertEqual(summary["rejected_error_count"], 3)
        self.assertEqual(summary["success_amount_usd"], "3865.38")
        self.assertEqual(summary["success_amount_vnd"], "99108343.20")
        self.assertEqual(summary["unknown_date_transactions"], 1)
        statuses = {r["final_status"]: r for r in read_csv(first["paths"]["by_status"])}
        self.assertEqual(len(statuses), 5)
        self.assertEqual(statuses["REJECTED_FRAUD"]["transaction_count"], "0")
        self.assertEqual(Decimal(statuses["REJECTED_ERROR"]["success_amount_usd"]), 0)
        days = read_csv(first["paths"]["by_day"])
        self.assertEqual([r["transaction_date"] for r in days], ["2019-12-31", "2020-01-01", "UNKNOWN_DATE"])
        self.assertEqual(sum(int(r["transaction_count"]) for r in days), 52)
        self.assertEqual(sum(Decimal(r["success_amount_usd"]) for r in days), Decimal("3865.38"))
        self.assertEqual(sum(Decimal(r["success_amount_vnd"]) for r in days), Decimal("99108343.20"))
        top = read_csv(first["paths"]["top_merchants"])
        self.assertEqual([r["merchant"] for r in top], ["UNKNOWN_MERCHANT", "Cửa hàng, B"])
        for name in ("by_status", "by_day", "top_merchants"):
            self.assertTrue(all(r["source_snapshot_id"] == source["snapshot_id"] for r in read_csv(first["paths"][name])))
        self.assertEqual(json.loads(path_of(first["paths"]["summary"]).read_text()), summary)
        original_summary = path_of(first["paths"]["summary"]).read_bytes()
        second, repeated = reports.build_reports(self.spark, str(self.base), top_n=2)
        self.assertNotEqual(first["report_id"], second["report_id"])
        self.assertEqual(json.loads((self.base / "reports/CURRENT.json").read_text()), second)
        self.assertEqual(repeated["success_amount_usd"], summary["success_amount_usd"])
        self.assertEqual(path_of(first["paths"]["summary"]).read_bytes(), original_summary)
        self.assertEqual((self.base / "curated/CURRENT.json").read_bytes(), source_before)

    def test_concurrent_curated_pointer_advance_keeps_selected_snapshot(self):
        source = self.baseline()
        old = self.base / "curated/snapshots/old_snapshot/manifest.json"
        select = reports.current_snapshot

        def select_then_advance(store, base):
            selected = select(store, base)
            (self.base / "curated/CURRENT.json").write_text(old.read_text())
            return selected

        with patch.object(reports, "current_snapshot", side_effect=select_then_advance):
            manifest, summary = reports.build_reports(self.spark, str(self.base))
        self.assertEqual(manifest["source_snapshot_id"], source["snapshot_id"])
        self.assertEqual(summary["transaction_count"], 52)
        self.assertEqual(summary["success_amount_usd"], "3865.38")

    def test_invalid_inputs_leave_previous_reports_current_unchanged(self):
        current = self.base / "reports/CURRENT.json"
        current.parent.mkdir(parents=True)
        current.write_text('{"report_id": "previous"}')
        before = current.read_bytes()
        cases = [
            ("duplicates", [transaction("same"), transaction("same")], "ID trung/thieu"),
            ("missing_money", [transaction("missing", None)], "so tien SUCCESS"),
            ("status", [transaction("status", status="UNKNOWN_STATUS")], "trang thai"),
            ("bad_total", [transaction("total")], "Tong success_amount_usd"),
            ("missing_id", [transaction("")], "ID trung/thieu"),
        ]
        for name, rows, message in cases:
            with self.subTest(name=name):
                source = self.snapshot(name, rows)
                if name == "bad_total":
                    source["success_amount_usd"] = "11.00"
                    text = json.dumps(source)
                    (self.base / "curated/CURRENT.json").write_text(text)
                    path_of(source["snapshot_path"]).joinpath("manifest.json").write_text(text)
                with self.assertRaisesRegex(RuntimeError, message):
                    reports.build_reports(self.spark, str(self.base))
                self.assertEqual(current.read_bytes(), before)

    def test_zero_success_and_negative_refunds(self):
        self.snapshot("rejected", [transaction("e", "999999.00", "REJECTED_ERROR", date=None)])
        manifest, summary = reports.build_reports(self.spark, str(self.base))
        self.assertEqual(summary["success_amount_usd"], "0.00")
        self.assertEqual(summary["success_amount_vnd"], "0.00")
        self.assertEqual(read_csv(manifest["paths"]["top_merchants"]), [])
        self.snapshot("refund", [transaction("refund", "-5.00", merchant="Refund"),
                                  transaction("zero", "0.00", merchant="Zero"),
                                  transaction("fraud", "10000.00", "REJECTED_FRAUD")])
        manifest, summary = reports.build_reports(self.spark, str(self.base))
        self.assertEqual(summary["success_amount_usd"], "-5.00")
        self.assertEqual(summary["success_amount_vnd"], "-128200.00")
        self.assertEqual([r["merchant"] for r in read_csv(manifest["paths"]["top_merchants"])], ["Zero", "Refund"])

    def test_failed_pointer_publication_preserves_previous_report(self):
        self.snapshot("source", [transaction("tx")])
        reports.build_reports(self.spark, str(self.base))
        current = self.base / "reports/CURRENT.json"
        before = current.read_bytes()
        rename = reports.HadoopStore.rename

        def fail_current(store, source, target, overwrite=False):
            if target.endswith("/reports/CURRENT.json"):
                raise OSError("simulated publication failure")
            return rename(store, source, target, overwrite)

        with patch.object(reports.HadoopStore, "rename", new=fail_current):
            with self.assertRaisesRegex(OSError, "simulated publication failure"):
                reports.build_reports(self.spark, str(self.base))
        self.assertEqual(current.read_bytes(), before)
        self.assertEqual(list((self.base / "reports/_staging").iterdir()), [])
        self.assertEqual(list((self.base / "reports").glob(".CURRENT_*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
