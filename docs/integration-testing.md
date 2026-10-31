# Kiểm thử tích hợp từng chặng

Hướng dẫn này dành cho WSL đã chạy được Kafka, Spark và HDFS. Không cần dừng
broker hoặc xóa checkpoint đang dùng. Các lệnh dưới đây do bạn chạy để quan sát
hệ thống; tài liệu và fixture không tự gửi dữ liệu tới dịch vụ nào.

Luồng cần hiểu:

```text
CSV → POS → SQLite PENDING → dispatcher → Kafka ACK → SQLite SENT
                                                  ↓
                                         Spark micro-batch
                                                  ↓
                                      CSV + metadata trên HDFS
                                                  ↓
                                curated loại trùng → reports thống kê
```

`SAVED` là SQLite đã commit. `SENT` là Kafka đã ACK, chưa có nghĩa Spark/HDFS
đã xử lý xong. `Batch ... completed` là callback đã publish đủ nhóm CSV;
Spark còn phải ghi commit vào checkpoint. Curated chỉ lấy batch đã commit.

## 1. Chuẩn bị bộ mẫu và môi trường thử riêng — terminal A

Tất cả terminal sử dụng môi trường WSL bạn đã kiểm tra. Tại thư mục gốc:

```bash
cd /home/hdoop/credit_card_project
source .venv/bin/activate
python - <<'PY'
import os
import shlex
import tempfile
from pathlib import Path
from dotenv import dotenv_values

project = Path.cwd()
config = dict(dotenv_values(project / '.env'))
run_directory = Path(tempfile.mkdtemp(prefix='credit-card-integration-'))
base = config['HDFS_BASE_PATH'].rstrip('/') + '_tests/' + run_directory.name
config.update({
    'CC_TEST_DIR': str(run_directory),
    'CSV_FILE': str(project / 'tests/fixtures/integration_transactions.csv'),
    'OUTBOX_DB_PATH': str(run_directory / 'outbox.sqlite3'),
    'KAFKA_TOPIC': 'credit-card-test-' + run_directory.name,
    'HDFS_BASE_PATH': base,
    'HDFS_CHECKPOINT_PATH': base + '/checkpoints/stream',
    'FX_RATE_FILE': str(run_directory / 'usd_vnd_rate.json'),
})
profile = run_directory / 'env.sh'
profile.write_text(''.join(
    f'export {key}={shlex.quote(str(value))}\n'
    for key, value in config.items() if value is not None
))
print('Dán dòng sau vào MỖI terminal dùng cho bài thử:')
print('source ' + shlex.quote(str(profile)))
PY
```

Dán dòng `source /tmp/credit-card-integration-.../env.sh` vừa được in ra vào
terminal A. Mỗi terminal mới cũng cần `cd`, kích hoạt `.venv` và dán CHÍNH
DÒNG SOURCE NÀY. Không chạy lại bước tạo profile ở terminal B, vì sẽ tạo bộ
outbox/topic/checkpoint khác.

```bash
printf '%s\n' "$CSV_FILE" "$OUTBOX_DB_PATH" "$KAFKA_TOPIC" "$HDFS_BASE_PATH"
```

Các biến môi trường này ghi đè cấu hình `.env` trong các process thử nghiệm;
không cần sửa `.env`. Profile chứa cấu hình thử trong `/tmp`, không đưa vào Git.
Muốn bắt đầu bài thử khác thì tạo profile mới; muốn kiểm tra restart thì giữ
nguyên profile cũ.

Fixture có 10 bản ghi, đánh số bản ghi dữ liệu từ 1 (không tính header):

| Bản ghi | Dữ liệu chính | Kết quả |
|---|---|---|
| 1 | Merchant A, 10 USD, 31/12/2019 | SUCCESS |
| 2 | Merchant A, 20 USD, 01/01/2020 | SUCCESS |
| 3 | Merchant B, 5 USD, Fraud=Yes | REJECTED_FRAUD |
| 4 | Merchant B, 15 USD, Errors=Bad PIN | REJECTED_ERROR |
| 5 | Merchant A, -2 USD | SUCCESS (hoàn tiền) |
| 6 | Merchant B, Fraud=Maybe | INVALID_FLAG |
| 7 | Merchant B, ngày 30/02/2020 | INVALID_DATA |
| 8 | Merchant B, giờ 99:99 | INVALID_DATA |
| 9 | Merchant B, 0 USD | SUCCESS |
| 10 | Merchant B, 12.50 USD | SUCCESS |

Không chỉnh fixture sau khi POS đã ghi cursor; checksum được dùng để phát
hiện CSV thay đổi giữa các lần chạy.

## 2. Tạo topic và lấy tỷ giá — terminal A

```bash
python - <<'PY'
import os
from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(
    bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(','),
    client_id='integration-setup',
)
try:
    topic = NewTopic(os.environ['KAFKA_TOPIC'], num_partitions=1, replication_factor=1)
    admin.create_topics([topic])
    print('Created:', os.environ['KAFKA_TOPIC'])
finally:
    admin.close()
PY
python rates/fetch_vcb_xml.py
```

Topic một partition giúp dễ theo dõi thứ tự và offset trong bài thử. File tỷ
giá phải được lưu vào thư mục thử `/tmp/...`, có ngày hiện tại. Nếu tải thất
bại, dừng tại đây để kiểm tra XML/HTML; không thay bằng tỷ giá tự bịa.

## 3. Quan sát POS và outbox — terminal A

Chưa chạy dispatcher hoặc `run_producer.py`. Chạy từng phần để quan sát rõ:

```bash
python producer/pos_simulator.py --limit 5
python producer/pos_simulator.py --status
```

Kết quả:

```text
PENDING: 5
SENT: 0
NEXT_CSV_LINE: 7
```

Mỗi dòng có một UUID và `SAVED`. Cursor bắt đầu ở 2 vì header là dòng 1.
Sau 5 bản ghi, vị trí tiếp theo là 7. Đây là số thứ tự bản ghi theo cách
script lưu cursor; CSV có ô nhiều dòng không đồng nghĩa với số dòng vật lý.

Xem payload và xác nhận ID khác nhau:

```bash
python - <<'PY'
import json
import os
import sqlite3
from contextlib import closing

with closing(sqlite3.connect(os.environ['OUTBOX_DB_PATH'])) as db:
    for transaction_id, payload, status in db.execute(
        'SELECT transaction_id, payload_json, status FROM outbox ORDER BY rowid'
    ):
        event = json.loads(payload)
        print(transaction_id, event['Amount'], event['Is Fraud?'], status)
PY
```

## 4. Kiểm tra khi chưa kết nối được Kafka — terminal A, tùy chọn

Giữ nguyên 5 bản ghi PENDING. Cố tình dùng cổng 1 cho riêng lệnh này:

```bash
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:1 python producer/kafka_dispatcher.py --limit 5
python producer/pos_simulator.py --status
```

Lệnh đầu dự kiến lỗi kết nối, có thể chờ một lúc rồi báo traceback. Sau đó
vẫn PENDING=5, SENT=0. Biến bootstrap chỉ đổi cho một lệnh; broker thật không
bị dừng. Bài thử này chứng minh outbox giữ dữ liệu khi kết nối thất bại;
không mô phỏng cửa sổ crash sau ACK nhưng trước cập nhật SQLite.

## 5. Gửi 5 sự kiện tới Kafka — terminal A

```bash
python producer/kafka_dispatcher.py --limit 5
python producer/pos_simulator.py --status
```

Kỳ vọng PENDING=0, SENT=5, cursor vẫn 7. Dispatcher chờ ngẫu nhiên 1–5 giây
trước mỗi lần gửi. Khoảng cách log thực tế còn gồm thời gian mạng/ACK, nên
không dùng nó để khẳng định mọi khoảng cách đều nhỏ hơn hoặc bằng 5 giây.

Lúc này chưa chạy Spark: dữ liệu vẫn được Kafka lưu chờ consumer.

Có thể đọc 5 bản tin để quan sát mà không thay đổi checkpoint của Spark:

```bash
python - <<'PY'
import json
import os
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    os.environ['KAFKA_TOPIC'],
    bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(','),
    group_id=None,
    enable_auto_commit=False,
    auto_offset_reset='earliest',
    consumer_timeout_ms=10000,
    value_deserializer=lambda value: json.loads(value.decode('utf-8')),
)
try:
    count = 0
    for message in consumer:
        event = message.value
        print(message.partition, message.offset, event['transaction_id'], event['Amount'])
        count += 1
        if count == 5:
            break
    assert count == 5, f'Expected 5 messages, got {count}'
finally:
    consumer.close()
PY
```

Kafka không xóa bản tin khi consumer này đọc. Với topic mới một partition,
offset thường là 0–4. Quan trọng là cùng topic + partition + offset xác định
một vị trí bản tin; transaction_id mới là ID nghiệp vụ để loại trùng gửi lại.

## 6. Khởi chạy Spark — terminal B

Mở terminal B, vào dự án, kích hoạt `.venv`, dán dòng source profile ở bước 1:

```bash
python run_streaming.py
```

Spark đọc 5 bản tin đã có. Tổng trạng thái qua tất cả batch lúc này:

```text
SUCCESS: 3
REJECTED_FRAUD: 1
REJECTED_ERROR: 1
```

Số batch, batch ID và số giao dịch mỗi batch không cố định; một batch có thể
chứa nhiều bản tin. Không yêu cầu một giao dịch tương ứng một batch.

## 7. Gửi 5 sự kiện còn lại trong lúc Spark chạy — terminal A

```bash
python producer/pos_simulator.py --limit 5
python producer/kafka_dispatcher.py --limit 5
python producer/pos_simulator.py --status
```

Kết quả cuối outbox:

```text
PENDING: 0
SENT: 10
NEXT_CSV_LINE: 12
```

Terminal B tiếp tục có log xử lý batch mới. Đợi xử lý hết; SENT=10 ở terminal
A chưa chứng minh Spark đã xử lý đủ 10.

## 8. Quan sát HDFS và quy đổi tiền — terminal A

```bash
hdfs dfs -ls -R "$HDFS_BASE_PATH/processed_v2"
hdfs dfs -cat "$HDFS_BASE_PATH/processed_v2/success/batch_id=*/part-*.csv"
hdfs dfs -cat "$HDFS_BASE_PATH/processed_v2/invalid/batch_id=*/part-*.csv"
hdfs dfs -ls "$HDFS_CHECKPOINT_PATH/commits"
```

Cần thấy:

- Các nhóm success/fraud/errors/invalid, các thư mục batch và `_SUCCESS`.
- Trong invalid: lý do INVALID_FRAUD_FLAG, INVALID_DATE, INVALID_TIME.
- Trong success: ngày dd/MM/yyyy, giờ HH:mm:ss, amount_numeric và amount_vnd.
- Metadata mỗi batch: input.json, fx.json nếu có SUCCESS, complete.json.
- Spark có commit log sau khi callback ghi xong. `_SUCCESS` của một nhóm riêng
  không chứng minh toàn bộ batch đã được Spark commit.

Mỗi CSV part có header riêng: không dùng `wc -l` hoặc cộng header để đếm giao
dịch. Tiền VNĐ là amount_numeric × fx_rate, làm tròn từng dòng tới 2 chữ số.
Các dòng không SUCCESS không được quy đổi. Nếu tất cả SUCCESS dùng cùng tỷ
giá R, tổng kỳ vọng là 40.50 × R; nếu khác tỷ giá, phải cộng từng dòng.

## 9. Build curated và reports — terminal A

```bash
python scripts/build_curated.py
python scripts/build_reports.py
hdfs dfs -cat "$HDFS_BASE_PATH/curated/CURRENT.json"
hdfs dfs -cat "$HDFS_BASE_PATH/reports/CURRENT.json"
```

Kết quả tổng quan kỳ vọng:

| Chỉ số | Giá trị |
|---|---:|
| SOURCE_ROWS | 10 |
| TRANSACTION_COUNT | 10 |
| MISSING_ID_ROWS | 0 |
| DUPLICATE_EXTRA_ROWS | 0 |
| SUCCESS_COUNT | 5 |
| REJECTED_FRAUD | 1 |
| REJECTED_ERROR | 1 |
| INVALID_FLAG | 1 |
| INVALID_DATA | 2 |
| SUCCESS_AMOUNT_USD | 40.50 |

Nếu chưa đủ 10, xem terminal B. Curated chọn một mốc batch đã commit lúc bắt
đầu chạy, nên cần đợi Spark xử lý hết và chạy lại CẢ curated lẫn reports.
Không sửa tay manifest để ép số liệu khớp.

Đọc các bảng và kiểm tra tổng VNĐ độc lập:

```bash
python - <<'PY'
import csv
import io
import json
import os
import subprocess
from decimal import Decimal, ROUND_HALF_UP

base = os.environ['HDFS_BASE_PATH']

def read_hdfs(path):
    result = subprocess.run(
        ['hdfs', 'dfs', '-cat', path], check=True, capture_output=True, text=True,
    )
    return result.stdout

def read_json(path):
    return json.loads(read_hdfs(path))

curated = read_json(base + '/curated/CURRENT.json')
reports = read_json(base + '/reports/CURRENT.json')
summary = read_json(reports['paths']['summary'])
assert summary['source_snapshot_id'] == curated['snapshot_id']
rows = list(csv.DictReader(io.StringIO(read_hdfs(curated['transactions_path'] + '/part-*.csv'))))
success = [row for row in rows if row['final_status'] == 'SUCCESS']
expected_vnd = sum((
    (Decimal(row['amount_numeric']) * Decimal(row['fx_rate'])).quantize(
        Decimal('0.01'), rounding=ROUND_HALF_UP,
    ) for row in success
), Decimal('0.00'))
assert summary['transaction_count'] == 10
assert summary['status_counts'] == {
    'SUCCESS': 5, 'REJECTED_FRAUD': 1, 'REJECTED_ERROR': 1,
    'INVALID_FLAG': 1, 'INVALID_DATA': 2,
}
assert Decimal(summary['success_amount_usd']) == Decimal('40.50')
assert Decimal(summary['success_amount_vnd']) == expected_vnd
print(json.dumps(summary, ensure_ascii=False, indent=2))
print('VNĐ đối chiếu:', expected_vnd)
for name in ('merchant_by_day', 'merchant_by_month', 'merchant_by_year'):
    print(name)
    print(read_hdfs(reports['paths'][name] + '/part-*.csv'))
PY
```

Merchant A có 3 SUCCESS, tổng 28 USD. Merchant B có 2 SUCCESS, tổng 12.50 USD.
Bảng merchant đếm mọi trạng thái ở transaction_count, đếm SUCCESS riêng ở
success_count; đây là hai chỉ số khác nhau. Giao dịch ngày lỗi nằm trong nhóm
UNKNOWN_DATE/MONTH/YEAR. Ngày báo cáo là ngày CSV (2019/2020), không phải ngày
hôm nay khi POS giả lập sự kiện.

## 10. Kiểm tra restart không tạo giao dịch mới

Ở terminal A:

```bash
python producer/pos_simulator.py --limit 5
python producer/kafka_dispatcher.py --limit 5
```

POS đã hết CSV: tạo thêm 0, SENT vẫn 10, cursor vẫn 12. Dispatcher báo không
có PENDING. Ở terminal B bấm Ctrl+C, đợi thoát hẳn rồi chạy lại:

```bash
python run_streaming.py
```

Giữ nguyên topic/output/checkpoint/profile. Không xóa checkpoint và không
tạo profile mới cho bài thử restart. Không phát sinh thêm giao dịch, tổng tiền
không đổi. Có thể chạy lại curated/reports và snippet đối chiếu ở bước 9;
report_id/snapshot_id/thời gian build sẽ đổi, các chỉ số nghiệp vụ phải giữ nguyên.

## 11. Kiểm tra gửi lặp cùng ID vẫn không cộng tiền hai lần

Terminal B đang chạy. Terminal A gửi lại nguyên payload đầu tiên từ outbox:

```bash
python - <<'PY'
import os
import sqlite3
from contextlib import closing
from kafka import KafkaProducer

with closing(sqlite3.connect(os.environ['OUTBOX_DB_PATH'])) as db:
    transaction_id, payload = db.execute(
        'SELECT transaction_id, payload_json FROM outbox ORDER BY rowid LIMIT 1'
    ).fetchone()
producer = KafkaProducer(
    bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(','),
    acks='all',
)
try:
    metadata = producer.send(
        os.environ['KAFKA_TOPIC'],
        key=transaction_id.encode('utf-8'),
        value=payload.encode('utf-8'),
    ).get(timeout=30)
    print('Resent SAME ID:', transaction_id, metadata.partition, metadata.offset)
finally:
    producer.close()
PY
```

Không tạo UUID hay timestamp mới; không sửa trạng thái outbox. Đây là bản gửi
lặp có chủ đích, mô phỏng việc gửi lại sau cửa sổ lỗi ACK → cập nhật SQLite.
Kafka có thêm một bản tin với offset mới; Spark có thêm một dòng đã xử lý.
Curated phải gộp nó vào transaction_id cũ và giữ bản lưu sớm nhất.

Đợi Spark commit rồi chạy lại curated/reports và kiểm tra bước 9. Kết quả:

```text
SOURCE_ROWS: 11
DUPLICATE_EXTRA_ROWS: 1
TRANSACTION_COUNT: 10
SUCCESS_COUNT: 5
SUCCESS_AMOUNT_USD: 40.50
```

Tổng VNĐ phải bằng giá trị đã ghi nhận ở bước 9. Outbox vẫn SENT=10 vì bạn chỉ
đọc lại một payload, không tạo POS event mới. So sánh này cho thấy sự khác nhau
giữa số lần gửi và số giao dịch nghiệp vụ.

## 12. Chạy kiểm thử tự động bổ sung

Ép filesystem mặc định về local cho riêng lệnh test, kể cả terminal đang có
cấu hình Hadoop thật:

```bash
PYSPARK_SUBMIT_ARGS='--conf spark.hadoop.fs.defaultFS=file:/// pyspark-shell' \
python -m unittest discover -s tests -v
```

Các test tự động còn kiểm tra rollback khi Ctrl+C, lỗi giữa các lần publish,
tỷ giá cũ, retry qua ngày và input khác dưới cùng batch ID. Chúng dùng dữ liệu
tạm và không thay thế việc quan sát Kafka/HDFS thật ở các bước trên.

Kết thúc: Ctrl+C streaming. Các biến thử chỉ có hiệu lực trong terminal đã
source profile; mở terminal mới không source profile để trở lại cấu hình gốc.
Không cần xóa topic/outbox/HDFS khi kết thúc để còn giữ bằng chứng kiểm thử.
