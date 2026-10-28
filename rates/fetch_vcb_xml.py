import json
import os
import tempfile
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

XML_URL = os.environ["VCB_XML_URL"]
HTML_URL = os.getenv(
    "VCB_HTML_URL",
    "https://www.vietcombank.com.vn/KHCN/Cong-cu-Tien-ich/Ty-gia",
)
TIMEOUT = float(os.getenv("FX_REQUEST_TIMEOUT_SECONDS", "20"))
RATE_TYPE = os.getenv("FX_RATE_TYPE", "transfer_buy")
BASE = os.getenv("FX_BASE_CURRENCY", "USD")
QUOTE = os.getenv("FX_QUOTE_CURRENCY", "VND")
TZ = ZoneInfo("Asia/Ho_Chi_Minh")

OUT = Path(os.environ["FX_RATE_FILE"])
if not OUT.is_absolute():
    OUT = ROOT / OUT

# Ten truong tuong ung trong XML va du lieu HTML.
FIELDS = {
    "transfer_buy": ("Transfer", "transfer"),
    "cash_buy": ("Buy", "cash"),
    "sell": ("Sell", "sell"),
}

if RATE_TYPE not in FIELDS:
    raise ValueError("FX_RATE_TYPE khong hop le")

if QUOTE != "VND":
    raise ValueError("Nguon Vietcombank nay quy doi ngoai te sang VND")


def download(url):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        return response.read()


def validate(raw_rate, published):
    try:
        rate = Decimal(str(raw_rate).replace(",", "").strip())
    except InvalidOperation as exc:
        raise ValueError("Ty gia khong phai so hop le") from exc

    if not rate.is_finite() or rate <= 0:
        raise ValueError("Ty gia phai huu han va lon hon 0")

    if not published:
        raise ValueError("Thieu ngay cong bo ty gia")

    try:
        published_at = datetime.fromisoformat(published)
    except ValueError:
        published_at = datetime.strptime(
            published,
            "%m/%d/%Y %I:%M:%S %p",
        )

    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=TZ)

    published_at = published_at.astimezone(TZ)

    if published_at.date() != datetime.now(TZ).date():
        raise ValueError("Ngay cong bo ty gia khong phai hom nay")

    return rate, published_at.isoformat(timespec="seconds")


def fetch_xml():
    root = ET.fromstring(download(XML_URL))
    field = FIELDS[RATE_TYPE][0]

    for item in root.iter("Exrate"):
        if item.get("CurrencyCode", "").upper() == BASE.upper():
            return validate(
                item.get(field),
                root.findtext("DateTime"),
            )

    raise ValueError(f"Khong tim thay {BASE} trong XML")


class RateHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.data = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)

        if tag == "input" and attrs.get("id") == "currentDataExchange":
            self.data = json.loads(attrs.get("value", ""))


def fetch_html():
    parser = RateHTMLParser()
    parser.feed(download(HTML_URL).decode("utf-8-sig"))

    if not isinstance(parser.data, dict):
        raise ValueError("Khong tim thay currentDataExchange trong HTML")

    field = FIELDS[RATE_TYPE][1]

    for item in parser.data.get("Data", []):
        if item.get("currencyCode", "").upper() == BASE.upper():
            return validate(
                item.get(field),
                parser.data.get("UpdatedDate"),
            )

    raise ValueError(f"Khong tim thay {BASE} trong HTML")


def save_record(record):
    OUT.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=OUT.parent,
            prefix=OUT.name + ".",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temp_path = Path(stream.name)
            json.dump(record, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())

        # Cong bo file moi sau khi ghi thanh cong.
        temp_path.replace(OUT)

    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def main():
    sources = [
        ("Vietcombank XML", XML_URL, fetch_xml),
        ("Vietcombank HTML", HTML_URL, fetch_html),
    ]

    for source, url, fetch in sources:
        try:
            rate, published = fetch()
        except Exception as exc:
            print(
                f"FAILED | {source} | {type(exc).__name__}: {exc}",
                flush=True,
            )
            continue

        record = {
            "base_currency": BASE,
            "quote_currency": QUOTE,
            "rate_type": RATE_TYPE,
            "rate": str(rate),
            "bank_published_at": published,
            "retrieved_at": datetime.now(TZ).isoformat(timespec="seconds"),
            "source": source,
            "source_url": url,
        }

        save_record(record)
        print(json.dumps(record, ensure_ascii=False, indent=2))
        print("Saved:", OUT, flush=True)
        return

    raise RuntimeError(
        "Ca XML va HTML deu loi. File ty gia cu duoc giu nguyen."
    )


if __name__ == "__main__":
    main()