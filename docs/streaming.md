# Streaming giao dịch

Chạy từ thư mục gốc bằng môi trường Python của dự án:

```bash
.venv/bin/python run_streaming.py
```

`run_streaming.py` nạp `.env`, thiết lập Python cho Spark, tìm cấu hình Hadoop
và nạp Kafka connector. Streaming kiểm tra output và checkpoint cùng nằm trên
HDFS; nếu filesystem thực tế là local thì dừng trước khi xử lý.

## Cấu trúc mã nguồn và thứ tự đọc

| File trong `streaming/` | Trách nhiệm |
|---|---|
| `transformations.py` | Schema JSON, kiểm tra ngày/giờ/tiền, lý do lỗi và phân loại giao dịch. |
| `fx_rates.py` | Kiểm tra cache và gọi bộ cập nhật tỷ giá khi cần. |
| `batch_storage.py` | Ghi metadata nguyên tử và ràng buộc checkpoint với output. |
| `batch_processor.py` | Xử lý micro-batch, cố định tỷ giá, ghi CSV qua staging và đánh dấu hoàn tất. |
| `parse_transactions.py` | Nạp cấu hình, khóa tiến trình, tạo Spark, nối Kafka với bộ xử lý batch và dừng chương trình. |

Đọc `transformations.py` trước để hiểu nghiệp vụ, sau đó `fx_rates.py`,
`batch_processor.py` và `batch_storage.py`. Cuối cùng đọc `parse_transactions.py`
để thấy cách các phần kết nối. Import các module không khởi chạy Spark/Kafka.

## Đọc luồng xử lý trong từng file

- `transform_transactions()`: `parse_kafka_messages()` → `normalize_transaction_fields()`
  → `add_validation_errors()` → `classify_transactions()`.
- `BatchProcessor.process_batch()`: đăng ký input → đếm trạng thái → lấy tỷ giá cố định
  → kiểm tra batch đã xong → quy đổi → publish các nhóm CSV → ghi complete.json.
  Mỗi bước là một method riêng; `BatchPaths` quản lý quy tắc đường dẫn.
- `FxRateProvider.get_record()`: đọc cache hợp lệ; nếu lỗi thì cập nhật một lần và đọc lại.
  Kiểm tra cặp tiền, giá trị và timestamp được tách riêng trong `validate_fx_record()`.
- `main()`: đọc `StreamingConfig` → giữ khóa tiến trình → tạo Spark → chạy stream
  → đóng Spark. `run_stream()` chịu trách nhiệm dừng query trong mọi trường hợp.

## Phân loại và định dạng

- JSON không parse được: `INVALID_DATA`, lưu payload gốc và lý do `MALFORMED_JSON`.
- `Is Fraud? = Yes`: `REJECTED_FRAUD`, không quy đổi tiền. Vẫn lưu để phân tích fraud.
- Cờ fraud ngoài Yes/No: `INVALID_FLAG`.
- Với giao dịch không fraud: ID, User, Card, merchant và city phải có giá trị;
  tiền tệ phải là USD. Ngày/giờ/số tiền sai: `INVALID_DATA`.
- Có `Errors?` khác rỗng: `REJECTED_ERROR`; còn lại `SUCCESS`.
- Ngày giao dịch lấy từ CSV, xuất `dd/MM/yyyy`. Giờ nhận `H:mm`, `HH:mm`
  hoặc có giây, xuất `HH:mm:ss`; không chấp nhận giờ ngoài phạm vi.
- Tiền được kiểm tra định dạng trước khi bỏ `$` và dấu phân cách hàng nghìn.
  Dùng `decimal(18,2)`, giữ số âm và 0, từ chối quá hai chữ số thập phân.
- `validation_errors` chứa các mã lỗi ngăn cách bởi `|`, kể cả lỗi phụ trong
  bản ghi fraud. `pos_event_time` là metadata mô phỏng, không thay thế ngày gốc.

Các CSV có thêm `topic`, `partition`, `kafka_timestamp`, `raw_value` và
`validation_errors`. Offset chỉ có ý nghĩa khi đi cùng topic và partition.
Các cột kỹ thuật này không tham gia so sánh nội dung nghiệp vụ khi loại trùng.
CSV mới dùng dấu nháy kép chuẩn; reader hỗ trợ trường có dấu nháy/xuống dòng.

## Tỷ giá hằng ngày

Batch mới có SUCCESS mới cần tỷ giá. Nếu cache thiếu, hỏng hoặc cũ, streaming
gọi `rates/fetch_vcb_xml.py` trên driver: XML trước, HTML dự phòng. Sau khi tải,
kiểm tra USD/VND, loại tỷ giá, số dương hữu hạn vừa `decimal(18,4)`, timezone,
ngày lấy/ngày công bố là hôm nay và không ở tương lai. Không tự dùng tỷ giá cũ
cho batch mới nếu cả hai nguồn lỗi; Spark dừng với batch chưa commit để chạy lại.
Batch chỉ có fraud/lỗi không cần tải tỷ giá.

Tỷ giá được cố định trong `processed_v2/_batches/batch_id=.../fx.json` trước khi
ghi CSV. Retry, kể cả qua ngày, dùng đúng bản đã cố định. Vì vậy không bắt buộc
refresh cache ngay lúc khởi động: ưu tiên cho phép hoàn tất batch cũ trước.
Quy đổi theo tỷ giá lúc xử lý mô phỏng, không tải tỷ giá lịch sử của năm trong CSV.

## Commit và phục hồi

Đường dẫn CSV giữ dạng `processed_v2/<success|fraud|errors|invalid>/batch_id=.../`.
Metadata cho batch mới:

- `input.json`: lineage, batch ID, số dòng, fingerprint và phạm vi offset từng partition.
- `fx.json`: tỷ giá cố định, chỉ có khi batch có SUCCESS.
- `complete.json`: danh sách nhóm đã ghi đầy đủ và số dòng tương ứng.

Mỗi thư mục CSV được ghi vào staging rồi rename sang đích; không overwrite
kết quả cũ. Khi retry, kiểm tra cùng input và giữ lại nhóm đã publish. Sau khi
mọi nhóm hoàn tất mới công bố complete.json; Spark sau đó commit checkpoint.
Curated chỉ đọc batch Spark đã commit, kiểm tra complete.json và số dòng thực tế.
Cơ chế gửi Kafka vẫn có thể tạo bản gửi lặp ở batch khác; curated tiếp tục loại
trùng theo transaction_id trước khi tính tổng.

Không xóa checkpoint để chạy lại cùng output. File `_stream.json` và
`checkpoint/credit_card_stream.json` ràng buộc vùng output với checkpoint.
Khóa cục bộ ngăn hai tiến trình streaming cùng dùng một vùng output trên máy
WSL; đây không phải khóa phân tán cho nhiều máy.

CSV cũ được đọc theo schema/escape cũ và bổ sung metadata thiếu bằng null.
Nếu nâng cấp đúng lúc một batch cũ đã ghi dở nhưng chưa commit, chương trình
báo `Batch cu chua co metadata` thay vì ghi đè. Khi đó cần hoàn tất batch bằng
phiên bản cũ hoặc dùng bộ topic/checkpoint/output riêng để chạy lại demo;
không xóa dữ liệu cũ để né kiểm tra này.

## Kiểm thử

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Test dùng dữ liệu tạm, Spark local và API Hadoop local: dữ liệu lỗi, Decimal,
cache tỷ giá, retry qua ngày, lỗi giữa các lần publish, checkpoint streaming
thực với nguồn file, CSV cũ/mới và luồng đến reports. Test không gửi Kafka,
không tải website thật và không ghi HDFS thật. Kiểm tra tích hợp Kafka/HDFS
và XML/HTML trực tiếp cần chạy trên môi trường triển khai của dự án.
