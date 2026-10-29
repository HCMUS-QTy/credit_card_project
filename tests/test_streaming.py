"""Streaming/curated integration checks on temporary local Spark/Hadoop storage."""

import csv
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from streaming import batch_storage, fx_rates, transformations
from streaming.batch_processor import BatchProcessor
from scripts import build_curated as curated, build_reports as reports


def rate_record(rate="25640", days=0):
    now = datetime.now(fx_rates.TZ) - timedelta(days=days, seconds=2)
    return {"base_currency": "USD", "quote_currency": "VND", "rate_type": "transfer_buy",
            "rate": rate, "retrieved_at": now.isoformat(), "bank_published_at": now.isoformat(),
            "source": "Test source", "source_url": "https://example.test/rates"}


def transaction(**updates):
    event = dict(zip(transformations.INPUT_COLUMNS, [
        "0", "0", "2020", "2", "29", "9:05", "$10.00", "Swipe", 'Shop, "A"', "City",
        "", "00123", "1", "", "No", "tx", "2026-10-29T10:00:00+07:00", "USD",
    ]))
    event.update(updates)
    return event


def csv_rows(path):
    rows = []
    for part in sorted(Path(path).glob("part-*.csv")):
        with part.open(newline="", encoding="utf-8") as file:
            rows.extend(csv.DictReader(file))
    return rows


class FxRateTests(unittest.TestCase):
    def test_reject_invalid_values_configuration_and_timestamps(self):
        cases = [{"rate": v} for v in ("0", "-1", "NaN", "Infinity", "1e14", "1.00001")]
        cases += [{"base_currency": "EUR"}, {"rate_type": "sell"},
                  {"bank_published_at": None}, {"retrieved_at": "2026-01-01T10:00:00"},
                  {"source": ""}, {"bank_published_at": (datetime.now(fx_rates.TZ)
                                                         + timedelta(days=1)).isoformat()}]
        for changes in cases:
            with self.subTest(changes=changes):
                with self.assertRaises(RuntimeError):
                    fx_rates.validate_fx_record(dict(rate_record(), **changes))
        self.assertEqual(fx_rates.validate_fx_record(rate_record()), Decimal("25640"))

    def test_old_rate_only_allowed_for_pinned_replay(self):
        old = rate_record(days=1)
        with self.assertRaises(RuntimeError):
            fx_rates.validate_fx_record(old)
        self.assertEqual(fx_rates.validate_fx_record(old, require_today=False), Decimal("25640"))

    def test_missing_or_stale_cache_refreshes_and_fresh_cache_is_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rate.json"
            for old in (None, rate_record(days=1), dict(rate_record(), bank_published_at=None)):
                with self.subTest(old=old):
                    if old is None:
                        path.unlink(missing_ok=True)
                    else:
                        path.write_text(json.dumps(old))
                    fresh = rate_record()
                    refresh = Mock(side_effect=lambda: path.write_text(json.dumps(fresh)))
                    provider = fx_rates.FxRateProvider(path, refresh=refresh)
                    self.assertEqual(provider.get_record(), fresh)
                    self.assertEqual(provider.get_record(), fresh)
                    refresh.assert_called_once()

    def test_failed_refresh_does_not_accept_stale_rate(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rate.json"
            path.write_text(json.dumps(rate_record(days=1)))
            refresh = Mock()
            with self.assertRaisesRegex(RuntimeError, "Khong co ty gia hom nay"):
                fx_rates.FxRateProvider(path, refresh=refresh).get_record()
            refresh.assert_called_once()


class StreamingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.spark = (SparkSession.builder.master("local[2]").appName("TestStreaming")
                     .config("spark.ui.enabled", "false")
                     .config("spark.ui.showConsoleProgress", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
                     .config("spark.sql.legacy.timeParserPolicy", "CORRECTED").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="credit-card-stream-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name) / "base"
        self.checkpoint = Path(temporary.name) / "checkpoint"
        self.provider = Mock(rate_type="transfer_buy")
        self.provider.get_record.return_value = rate_record()
        self.processor = BatchProcessor(self.spark, str(self.base), self.provider, "test-lineage")

    def dataframe(self, events):
        rows = [("transactions", 0, i, datetime(2026, 10, 29, 3),
                 json.dumps(event, ensure_ascii=False, indent=2) if isinstance(event, dict) else event)
                for i, event in enumerate(events)]
        raw = self.spark.createDataFrame(rows,
            "topic string, partition int, offset long, timestamp timestamp, value string")
        return transformations.transform_transactions(raw)

    def committed(self, batch_id):
        path = self.checkpoint / "commits" / str(batch_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()

    def test_validation_and_statuses_even_with_ansi_enabled(self):
        changes = [
            ({}, "SUCCESS", None),
            ({"Time": "12:34:56"}, "SUCCESS", None),
            ({"Amount": "$-2.00"}, "SUCCESS", None),
            ({"Amount": "$1,234.56"}, "SUCCESS", None),
            ({"Amount": "$0.00"}, "SUCCESS", None),
            ({"transaction_id": None}, "INVALID_DATA", "MISSING_ID"),
            ({"User": ""}, "INVALID_DATA", "MISSING_USER"),
            ({"Card": ""}, "INVALID_DATA", "MISSING_CARD"),
            ({"Merchant Name": " "}, "INVALID_DATA", "MISSING_MERCHANT"),
            ({"Merchant City": None}, "INVALID_DATA", "MISSING_CITY"),
            ({"currency": "EUR"}, "INVALID_DATA", "UNSUPPORTED_CURRENCY"),
            ({"Time": "99:99"}, "INVALID_DATA", "INVALID_TIME"),
            ({"Month": "123"}, "INVALID_DATA", "INVALID_DATE"),
            ({"Year": "20200"}, "INVALID_DATA", "INVALID_DATE"),
            ({"Year": "0000"}, "INVALID_DATA", "INVALID_DATE"),
            ({"Year": "2019"}, "INVALID_DATA", "INVALID_DATE"),
            ({"Amount": "$1,2"}, "INVALID_DATA", "INVALID_AMOUNT"),
            ({"Amount": "$1.234"}, "INVALID_DATA", "INVALID_AMOUNT"),
            ({"Amount": "$999999999999999999999"}, "INVALID_DATA", "INVALID_AMOUNT"),
            ({"Is Fraud?": " yes ", "Amount": "bad"}, "REJECTED_FRAUD", "INVALID_AMOUNT"),
            ({"Is Fraud?": "unknown"}, "INVALID_FLAG", "INVALID_FRAUD_FLAG"),
            ({"Errors?": "Bad PIN"}, "REJECTED_ERROR", None),
        ]
        old_ansi = self.spark.conf.get("spark.sql.ansi.enabled")
        self.spark.conf.set("spark.sql.ansi.enabled", "true")
        try:
            rows = self.dataframe([transaction(**case) for case, _, _ in changes] + ["{broken", None]).orderBy("offset").collect()
        finally:
            self.spark.conf.set("spark.sql.ansi.enabled", old_ansi)
        for row, (changeset, status, error) in zip(rows, changes):
            with self.subTest(changes=changeset):
                self.assertEqual(row.final_status, status)
                if error:
                    self.assertIn(error, row.validation_errors)
        self.assertEqual(rows[0].date_formatted, "29/02/2020")
        self.assertEqual(rows[0].time_formatted, "09:05:00")
        self.assertEqual(rows[1].time_formatted, "12:34:56")
        self.assertEqual(rows[2].amount_numeric, Decimal("-2.00"))
        self.assertEqual(rows[3].amount_numeric, Decimal("1234.56"))
        self.assertEqual(rows[0].topic, "transactions")
        self.assertIn("\n", rows[0].raw_value)
        for row in rows[-2:]:
            self.assertEqual(row.final_status, "INVALID_DATA")
            self.assertIn("MALFORMED_JSON", row.validation_errors)

    def test_retry_after_partial_publication_pins_fx_and_curated_waits(self):
        events = [transaction(), transaction(**{"transaction_id": "fraud", "Is Fraud?": "Yes"})]
        df = self.dataframe(events)
        rename = self.processor.store.rename

        def fail_fraud(source, target, overwrite=False):
            if "/fraud/batch_id=" in target:
                raise OSError("simulated publication failure")
            return rename(source, target, overwrite)

        with patch.object(self.processor.store, "rename", side_effect=fail_fraud):
            with self.assertRaisesRegex(OSError, "simulated"):
                self.processor.process_batch(df, 0)
        success = self.base / "processed_v2/success/batch_id=0000000000"
        before = {p.name: p.read_bytes() for p in success.glob("part-*.csv")}
        complete = self.base / "processed_v2/_batches/batch_id=0000000000/complete.json"
        self.assertFalse(complete.exists())
        self.committed(0)
        with self.assertRaisesRegex(RuntimeError, "thieu complete.json"):
            curated.discover_inputs(self.processor.store, str(self.base), str(self.checkpoint))
        self.provider.get_record.return_value = rate_record("26000")
        tomorrow = datetime.now(fx_rates.TZ) + timedelta(days=1)

        class Tomorrow(datetime):
            @classmethod
            def now(cls, tz=None):
                return tomorrow.astimezone(tz) if tz else tomorrow.replace(tzinfo=None)

        with patch.object(fx_rates, "datetime", Tomorrow):
            self.processor.process_batch(df, 0)
        self.assertTrue(complete.exists())
        self.assertEqual(csv_rows(success)[0]["amount_vnd"], "256400.00")
        self.provider.get_record.assert_called_once()
        self.processor.process_batch(df, 0)
        self.assertEqual(before, {p.name: p.read_bytes() for p in success.glob("part-*.csv")})
        self.assertEqual(list((self.base / "processed_v2/_staging").iterdir()), [])

    def test_pipeline_to_curated_and_reports_preserves_raw_and_refunds(self):
        df = self.dataframe([
            transaction(), transaction(**{"transaction_id": "refund", "Amount": "$-2.00"}),
            transaction(**{"transaction_id": "fraud", "Is Fraud?": "Yes"}),
            transaction(transaction_id=None),
        ])
        self.processor.process_batch(df, 0)
        self.committed(0)
        snapshot = curated.build_snapshot(self.spark, str(self.base), str(self.checkpoint))
        self.assertEqual(snapshot["source_rows"], 4)
        self.assertEqual(snapshot["curated_transactions"], 3)
        self.assertEqual(snapshot["missing_id_rows"], 1)
        manifest, summary = reports.build_reports(self.spark, str(self.base))
        self.assertEqual(summary["success_amount_usd"], "8.00")
        self.assertEqual(summary["success_amount_vnd"], "205120.00")
        self.assertIn("merchant_by_month", manifest["paths"])
        fraud = csv_rows(self.base / "processed_v2/fraud/batch_id=0000000000")[0]
        self.assertEqual(fraud["amount_vnd"], "")

    def test_real_file_stream_checkpoint_restart(self):
        directory = self.base.parent / "input"
        directory.mkdir()
        schema = "topic string, partition int, offset long, timestamp timestamp, value string"
        lineage = batch_storage.bind_lineage(self.processor.store, str(self.base), str(self.checkpoint))
        processor = BatchProcessor(self.spark, str(self.base), self.provider, lineage)
        for index in range(2):
            event = transaction(transaction_id=f"stream-{index}")
            record = {"topic": "transactions", "partition": 0, "offset": index,
                      "timestamp": "2026-10-29T10:00:00+07:00", "value": json.dumps(event)}
            (directory / f"input-{index}.json").write_text(json.dumps(record) + "\n")
            raw = self.spark.readStream.schema(schema).json(str(directory))
            query = (transformations.transform_transactions(raw).writeStream
                     .foreachBatch(processor.process_batch)
                     .option("checkpointLocation", str(self.checkpoint))
                     .trigger(availableNow=True).start())
            try:
                self.assertTrue(query.awaitTermination(60))
            finally:
                query.stop()
            self.assertEqual(batch_storage.bind_lineage(self.processor.store, str(self.base),
                                                str(self.checkpoint)), lineage)
        result = curated.build_snapshot(self.spark, str(self.base), str(self.checkpoint))
        self.assertEqual(result["source_rows"], 2)
        self.assertEqual(result["curated_transactions"], 2)
        self.assertEqual(result["source_committed_batch"], 1)

    def test_changed_input_cannot_overwrite_same_batch(self):
        self.processor.process_batch(self.dataframe([transaction()]), 0)
        with self.assertRaisesRegex(RuntimeError, "input/lineage khac"):
            self.processor.process_batch(self.dataframe([transaction(Amount="$20.00")]), 0)
        rows = csv_rows(self.base / "processed_v2/success/batch_id=0000000000")
        self.assertEqual(rows[0]["amount_vnd"], "256400.00")

    def test_fraud_only_batch_needs_no_fx(self):
        self.provider.get_record.side_effect = AssertionError("FX should not be requested")
        self.processor.process_batch(self.dataframe([transaction(**{"Is Fraud?": "Yes"})]), 0)
        self.provider.get_record.assert_not_called()
        self.assertFalse((self.base / "processed_v2/success").exists())

    def test_fx_failure_leaves_batch_unpublished_and_retryable(self):
        self.provider.get_record.side_effect = RuntimeError("both sources unavailable")
        df = self.dataframe([transaction()])
        with self.assertRaisesRegex(RuntimeError, "both sources"):
            self.processor.process_batch(df, 0)
        self.assertFalse((self.base / "processed_v2/success").exists())
        self.assertFalse((self.base / "processed_v2/_batches/batch_id=0000000000/complete.json").exists())
        self.provider.get_record.side_effect = None
        self.processor.process_batch(df, 0)
        self.assertEqual(len(csv_rows(self.base / "processed_v2/success/batch_id=0000000000")), 1)

    def test_lineage_rejects_lost_checkpoint_and_legacy_batch_overwrite(self):
        store = self.processor.store
        identity = batch_storage.bind_lineage(store, str(self.base), str(self.checkpoint))
        self.assertEqual(batch_storage.bind_lineage(store, str(self.base), str(self.checkpoint)), identity)
        (self.checkpoint / "credit_card_stream.json").unlink()
        with self.assertRaisesRegex(RuntimeError, "Checkpoint khong khop"):
            batch_storage.bind_lineage(store, str(self.base), str(self.checkpoint))
        legacy = self.base / "processed_v2/success/batch_id=0000000000"
        legacy.mkdir(parents=True)
        with self.assertRaisesRegex(RuntimeError, "Batch cu chua co metadata"):
            self.processor.process_batch(self.dataframe([transaction()]), 0)

    def test_curated_mixed_headers_and_replay_metadata_do_not_conflict(self):
        first = self.dataframe([transaction()])
        self.processor.process_batch(first, 0)
        new = self.base / "processed_v2/success/batch_id=0000000000"
        legacy = self.base / "processed_v2/success/batch_id=0000000001"
        legacy.mkdir(parents=True)
        row = csv_rows(new)[0]
        row["offset"] = "9"
        fields = curated.CORE_COLUMNS + curated.FX_COLUMNS
        with (legacy / "part-00000.csv").open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fields, escapechar="\\", doublequote=False)
            writer.writeheader()
            writer.writerow({name: row[name] for name in fields})
        (legacy / "_SUCCESS").touch()
        self.committed(1)
        snapshot = curated.build_snapshot(self.spark, str(self.base), str(self.checkpoint))
        self.assertEqual(snapshot["curated_transactions"], 1)
        self.assertEqual(snapshot["duplicate_extra_rows"], 1)
        self.assertEqual(snapshot["source_rows"], 2)


if __name__ == "__main__":
    unittest.main()
