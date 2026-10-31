"""Exporter checks on temporary snapshots; no Spark, Kafka or HDFS required."""

import csv
import hashlib
import json
import sys
import tempfile
import unittest
from collections import Counter
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import export_powerbi as powerbi


class LocalStore:
    def __init__(self):
        self.current_reads = 0
        self.after_current_read = None

    def path(self, path):
        parsed = urlsplit(str(path))
        return Path(unquote(parsed.path)) if parsed.scheme else Path(path)

    def qualified(self, path):
        return self.path(path).resolve().as_uri()

    def read_json(self, path):
        record = json.loads(self.path(path).read_text())
        if str(path).endswith('/curated/CURRENT.json'):
            self.current_reads += 1
            if self.after_current_read:
                self.after_current_read()
        return record

    def read_text(self, path):
        return self.path(path).read_text(encoding='utf-8')

    def exists(self, path):
        return self.path(path).exists()

    def csv_parts(self, directory):
        return [str(path) for path in sorted(self.path(directory).glob('part-*.csv'))]


def transaction(tx_id, amount='10.00', status='SUCCESS', merchant='Shop A', date='31/12/2019'):
    success = status == 'SUCCESS'
    return {
        'transaction_id': tx_id, 'User': '0007', 'Card': '0012',
        'date_formatted': date, 'time_formatted': '09:05:00',
        'merchant': merchant, 'city': 'HANOI', 'final_status': status, 'currency': 'USD',
        'amount_numeric': amount,
        'amount_vnd': str(Decimal(amount) * Decimal('25640')) if success and amount is not None else '',
        'fx_rate': '25640.0000' if success else '',
        'raw_value': '{\n"note": "Cửa hàng"\n}',
    }


class ExportPowerBITests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='powerbi-export-')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name) / 'lake'
        self.output = Path(temporary.name) / 'export'
        self.store = LocalStore()

    def snapshot(self, name, rows, duplicates=0, legacy=False, parts=1):
        snapshot = self.base / 'curated/snapshots' / name
        data = snapshot / 'transactions'
        data.mkdir(parents=True)
        fields = [column for column in powerbi.EXPORT_COLUMNS if column != 'source_snapshot_id']
        if not legacy:
            fields.append('raw_value')
        for index in range(parts):
            with (data / f'part-{index:05d}.csv').open('w', encoding='utf-8', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fields, extrasaction='ignore',
                                        escapechar='\\' if legacy else None, doublequote=not legacy)
                writer.writeheader()
                writer.writerows(rows[index::parts])
        (data / '_SUCCESS').touch()
        success = [row for row in rows if row['final_status'] == 'SUCCESS']
        source = {
            'format_version': 1, 'snapshot_id': name, 'source_committed_batch': 6,
            'source_rows': len(rows) + duplicates, 'missing_id_rows': 0,
            'duplicate_extra_rows': duplicates, 'curated_transactions': len(rows),
            'status_counts': dict(Counter(row['final_status'] for row in rows)),
            'success_amount_usd': str(sum((Decimal(row['amount_numeric'] or '0') for row in success), Decimal('0.00'))),
            'success_amount_vnd': str(sum((Decimal(row['amount_vnd'] or '0') for row in success), Decimal('0.00'))),
            'snapshot_path': snapshot.as_uri(), 'transactions_path': data.as_uri(),
        }
        self.write_manifest(source)
        return source

    def write_manifest(self, source):
        text = json.dumps(source, ensure_ascii=False)
        self.store.path(source['snapshot_path']).joinpath('manifest.json').write_text(text)
        (self.base / 'curated/CURRENT.json').write_text(text)

    def export(self):
        return powerbi.export_powerbi(self.store, str(self.base), self.output)

    def csv_rows(self, manifest):
        with (self.output / manifest['csv']['path']).open(encoding='utf-8', newline='') as file:
            return list(csv.DictReader(file))

    def baseline(self):
        self.snapshot('baseline', [transaction('sale'), transaction('refund', '-2.00'),
                                  transaction('fraud', '999.00', 'REJECTED_FRAUD', date='')], duplicates=1)
        return self.export()

    def test_totals_identifiers_refunds_provenance_and_repeat(self):
        first = self.baseline()
        rows = self.csv_rows(first)
        self.assertEqual(first['transaction_count'], 3)
        self.assertEqual(first['source_rows'], 4)
        self.assertEqual(first['duplicate_extra_rows'], 1)
        self.assertEqual(first['status_counts']['SUCCESS'], 2)
        self.assertEqual(first['success_amount_usd'], '8.00')
        self.assertEqual(first['success_amount_vnd'], '205120.00')
        self.assertEqual(rows[0]['User'], '0007')
        self.assertEqual(rows[0]['Card'], '0012')
        self.assertEqual(rows[1]['amount_numeric'], '-2.00')
        self.assertEqual(rows[2]['date_formatted'], '')
        self.assertEqual(rows[2]['amount_vnd'], '')
        self.assertTrue(all(row['source_snapshot_id'] == 'baseline' for row in rows))
        self.assertNotIn('raw_value', rows[0])
        csv_before = (self.output / first['csv']['path']).read_bytes()
        self.assertEqual(first['csv']['sha256'], hashlib.sha256(csv_before).hexdigest())
        self.assertEqual(json.loads((self.output / first['manifest_path']).read_text()), first)
        second = self.export()
        self.assertNotEqual(first['export_id'], second['export_id'])
        self.assertEqual((self.output / second['csv']['path']).read_bytes(), csv_before)
        self.assertEqual(json.loads((self.output / 'CURRENT.json').read_text()), second)
        self.assertEqual((self.output / first['csv']['path']).read_bytes(), csv_before)

    def test_current_is_read_once_when_source_advances(self):
        self.snapshot('older', [transaction('old', '100.00')])
        newer = self.snapshot('newer', [transaction('new', '20.00')])
        old_text = (self.base / 'curated/snapshots/older/manifest.json').read_text()
        (self.base / 'curated/CURRENT.json').write_text(old_text)
        self.store.after_current_read = lambda: (self.base / 'curated/CURRENT.json').write_text(json.dumps(newer))
        manifest = self.export()
        self.assertEqual(self.store.current_reads, 1)
        self.assertEqual(manifest['source_snapshot_id'], 'older')
        self.assertEqual(manifest['success_amount_usd'], '100.00')
        self.assertEqual(self.csv_rows(manifest)[0]['transaction_id'], 'old')

    def test_invalid_rows_preserve_previous_export(self):
        previous = self.baseline()
        before = (self.output / 'CURRENT.json').read_bytes()
        cases = {
            'missing_id': [dict(transaction('valid'), transaction_id='')],
            'duplicate_id': [transaction('same'), transaction('same')],
            'status': [dict(transaction('valid'), final_status='UNKNOWN')],
            'currency': [dict(transaction('valid'), currency='EUR')],
            'missing_money': [transaction('valid', None)],
            'wrong_vnd': [dict(transaction('valid'), amount_vnd='1.00')],
            'bad_rate': [dict(transaction('valid'), fx_rate='NaN')],
            'zero_rate': [dict(transaction('valid'), fx_rate='0')],
            'precision': [dict(transaction('valid'), amount_numeric='1.001')],
            'powerbi_overflow': [dict(transaction('valid'), amount_vnd='999999999999999.00')],
        }
        for name, rows in cases.items():
            with self.subTest(name=name):
                self.snapshot(name, rows)
                with self.assertRaises(RuntimeError):
                    self.export()
                self.assertEqual((self.output / 'CURRENT.json').read_bytes(), before)
                self.assertTrue((self.output / previous['csv']['path']).exists())
                self.assertEqual(list((self.output / '_staging').iterdir()), [])

    def test_manifest_mismatch_preserves_previous_export(self):
        self.baseline()
        before = (self.output / 'CURRENT.json').read_bytes()
        changes = [
            {'curated_transactions': 2}, {'status_counts': {'REJECTED_ERROR': 1}},
            {'success_amount_usd': '999.00'}, {'success_amount_vnd': '999.00'},
            {'source_rows': 9},
        ]
        for index, change in enumerate(changes):
            with self.subTest(change=change):
                source = self.snapshot(f'mismatch_{index}', [transaction('sale')])
                source.update(change)
                self.write_manifest(source)
                with self.assertRaises(RuntimeError):
                    self.export()
                self.assertEqual((self.output / 'CURRENT.json').read_bytes(), before)

    def test_unfinished_and_mismatched_snapshot_is_rejected(self):
        source = self.snapshot('incomplete', [transaction('sale')])
        self.store.path(source['transactions_path']).joinpath('_SUCCESS').unlink()
        with self.assertRaisesRegex(RuntimeError, 'chua hoan tat'):
            self.export()
        source = self.snapshot('tampered', [transaction('sale')])
        manifest = self.store.path(source['snapshot_path']) / 'manifest.json'
        manifest.write_text('{}')
        with self.assertRaisesRegex(RuntimeError, 'CURRENT khong khop'):
            self.export()
        self.assertFalse((self.output / 'CURRENT.json').exists())

    def test_pointer_publication_failure_keeps_previous_bundle(self):
        previous = self.baseline()
        before = (self.output / 'CURRENT.json').read_bytes()
        self.snapshot('next', [transaction('next', '20.00')])
        with patch.object(powerbi.os, 'replace', side_effect=OSError('publication failed')):
            with self.assertRaisesRegex(OSError, 'publication failed'):
                self.export()
        self.assertEqual((self.output / 'CURRENT.json').read_bytes(), before)
        self.assertTrue((self.output / previous['csv']['path']).exists())
        self.assertEqual(list(self.output.glob('.CURRENT_*.tmp')), [])

    def test_zero_success_and_empty_snapshot(self):
        self.snapshot('fraud_only', [transaction('fraud', status='REJECTED_FRAUD')])
        manifest = self.export()
        self.assertEqual(manifest['success_amount_usd'], '0.00')
        self.assertEqual(manifest['success_amount_vnd'], '0.00')
        self.snapshot('empty', [])
        manifest = self.export()
        self.assertEqual(manifest['transaction_count'], 0)
        self.assertEqual(self.csv_rows(manifest), [])

    def test_unicode_quotes_multiline_and_legacy_parts(self):
        merchant = 'Cửa hàng, "A"\nChi nhánh'
        for index, legacy in enumerate((False, True)):
            with self.subTest(legacy=legacy):
                self.snapshot(f'encoding_{index}', [transaction('a', merchant=merchant),
                              transaction('b', '0.00')], legacy=legacy, parts=2)
                manifest = self.export()
                self.assertEqual(self.csv_rows(manifest)[0]['merchant'], merchant)
                self.assertEqual(manifest['transaction_count'], 2)

    def test_replay_delivery_counts_do_not_inflate_export(self):
        self.snapshot('before_replay', [transaction('sale')])
        before = self.export()
        self.snapshot('after_replay', [transaction('sale')], duplicates=1)
        after = self.export()
        self.assertEqual(before['transaction_count'], after['transaction_count'])
        self.assertEqual(before['success_amount_vnd'], after['success_amount_vnd'])
        self.assertEqual(after['source_rows'], 2)
        self.assertEqual(after['duplicate_extra_rows'], 1)

    def test_export_lock_serializes_writers(self):
        self.snapshot('source', [transaction('sale')])
        with powerbi.export_lock(self.output):
            with self.assertRaisesRegex(RuntimeError, 'export Power BI khac'):
                self.export()
        self.assertEqual(self.export()['transaction_count'], 1)


if __name__ == '__main__':
    unittest.main()
