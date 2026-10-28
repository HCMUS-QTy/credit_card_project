
import json
import os
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

url = os.environ["VCB_XML_URL"]
timeout = float(os.getenv("FX_REQUEST_TIMEOUT_SECONDS", "20"))
rate_type = os.getenv("FX_RATE_TYPE", "transfer_buy")
base_currency = os.getenv("FX_BASE_CURRENCY", "USD")
quote_currency = os.getenv("FX_QUOTE_CURRENCY", "VND")

out = Path(os.environ["FX_RATE_FILE"])
if not out.is_absolute():
    out = ROOT / out

field = {
    "transfer_buy": "Transfer",
    "cash_buy": "Buy",
    "sell": "Sell"
}.get(rate_type)

if field is None:
    raise ValueError("FX_RATE_TYPE khong hop le")

request = urllib.request.Request(
    url,
    headers={"User-Agent": "Mozilla/5.0"}
)

with urllib.request.urlopen(request, timeout=timeout) as response:
    root = ET.fromstring(response.read())

usd = next(
    (
        item for item in root.iter("Exrate")
        if item.attrib.get("CurrencyCode", "").upper()
        == base_currency.upper()
    ),
    None
)

if usd is None:
    raise RuntimeError("Khong tim thay USD trong XML")

raw_rate = usd.attrib.get(field, "").replace(",", "").strip()

try:
    rate = Decimal(raw_rate)
except InvalidOperation as exc:
    raise RuntimeError("Ty gia khong hop le") from exc

if not rate.is_finite() or rate <= 0:
    raise RuntimeError("Ty gia phai lon hon 0")

record = {
    "base_currency": base_currency,
    "quote_currency": quote_currency,
    "rate_type": rate_type,
    "rate": str(rate),
    "bank_published_at": root.findtext("DateTime"),
    "retrieved_at": datetime.now(
        ZoneInfo("Asia/Ho_Chi_Minh")
    ).isoformat(timespec="seconds"),
    "source": "Vietcombank XML",
    "source_url": url
}

out.parent.mkdir(parents=True, exist_ok=True)

temp_file = out.with_name(out.name + ".tmp")
temp_file.write_text(
    json.dumps(record, ensure_ascii=False, indent=2),
    encoding="utf-8"
)
temp_file.replace(out)

print(json.dumps(record, ensure_ascii=False, indent=2))
print("Saved:", out)
