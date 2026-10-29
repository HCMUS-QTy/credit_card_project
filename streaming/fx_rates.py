"""Kiểm tra bản tỷ giá và làm mới cache khi một batch mới cần quy đổi."""

import json
import os
import subprocess
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TZ = ZoneInfo("Asia/Ho_Chi_Minh")
RATE_LIMIT = Decimal("100000000000000")
RATE_QUANTUM = Decimal("0.0001")


def validate_fx_record(record, rate_type="transfer_buy", now=None, require_today=True):
    """Trả về Decimal hợp lệ; batch retry được phép dùng ngày đã cố định."""
    current_time = (now or datetime.now(TZ)).astimezone(TZ)
    try:
        _validate_currency_settings(record, rate_type)
        rate = _parse_rate(record["rate"])
        _validate_timestamps(record, current_time, require_today)
        if not record["source"] or not record["source_url"]:
            raise ValueError("Thieu nguon ty gia")
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise RuntimeError(f"Ty gia khong hop le: {exc}") from exc
    return rate


def _validate_currency_settings(record: dict, rate_type: str) -> None:
    actual = (record["base_currency"], record["quote_currency"], record["rate_type"])
    expected = ("USD", "VND", rate_type)
    if actual != expected:
        raise ValueError("Cau hinh ty gia khong khop USD/VND")


def _parse_rate(raw_rate) -> Decimal:
    rate = Decimal(raw_rate)
    if not rate.is_finite() or rate <= 0:
        raise ValueError("Ty gia phai huu han va lon hon 0")
    if rate >= RATE_LIMIT or rate != rate.quantize(RATE_QUANTUM):
        raise ValueError("Ty gia khong vua decimal(18,4)")
    return rate


def _parse_timestamp(record, field, current_time, require_today) -> datetime:
    timestamp = datetime.fromisoformat(record[field])
    if timestamp.tzinfo is None:
        raise ValueError(f"{field} thieu mui gio")
    timestamp = timestamp.astimezone(TZ)
    if timestamp > current_time:
        raise ValueError(f"{field} o tuong lai")
    if require_today and timestamp.date() != current_time.date():
        raise ValueError(f"{field} khong phai hom nay")
    return timestamp


def _validate_timestamps(record, current_time, require_today) -> None:
    retrieved_at = _parse_timestamp(record, "retrieved_at", current_time, require_today)
    published_at = _parse_timestamp(record, "bank_published_at", current_time, require_today)
    if published_at > retrieved_at:
        raise ValueError("Ngay cong bo sau ngay lay ty gia")


class FxRateProvider:
    def __init__(self, path, rate_type="transfer_buy", refresh=None):
        self.path = Path(path)
        self.rate_type = rate_type
        self.refresh = refresh or self._refresh

    def get_record(self) -> dict:
        """Đọc cache; nếu không hợp lệ thì cập nhật một lần và kiểm tra lại."""
        try:
            return self._read_valid_record()
        except (OSError, ValueError, RuntimeError) as exc:
            print(f"FX cache can cap nhat: {exc}", flush=True)

        self.refresh()
        try:
            return self._read_valid_record()
        except (OSError, ValueError, RuntimeError) as exc:
            raise RuntimeError("Khong co ty gia hom nay sau khi cap nhat") from exc

    def _read_valid_record(self) -> dict:
        content = self.path.read_text(encoding="utf-8")
        record = json.loads(content)
        validate_fx_record(record, self.rate_type)
        return record

    def _refresh(self) -> None:
        environment = os.environ.copy()
        environment.update({
            "FX_RATE_FILE": str(self.path),
            "FX_BASE_CURRENCY": "USD",
            "FX_QUOTE_CURRENCY": "VND",
            "FX_RATE_TYPE": self.rate_type,
        })
        command = [sys.executable, str(PROJECT_ROOT / "rates/fetch_vcb_xml.py")]
        subprocess.run(command, cwd=PROJECT_ROOT, env=environment, check=True)
