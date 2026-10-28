# Báo cáo từ curated

`scripts/build_reports.py` đọc đúng snapshot được chỉ định trong
`curated/CURRENT.json` và tạo báo cáo trên HDFS. Script dùng cùng môi trường
Python, Java và Hadoop với `scripts/build_curated.py`; không cần thư viện mới.

## Chạy trên WSL

Tại thư mục gốc dự án, kích hoạt môi trường Python đang dùng:

```bash
source .venv/bin/activate
python scripts/build_reports.py
```

Mặc định xuất 10 merchant có tổng VND của giao dịch thành công cao nhất.
Đổi số lượng bằng:

```bash
python scripts/build_reports.py --top-n 20
```

Script đọc `.env` ở thư mục gốc. Cần `HDFS_BASE_PATH`; `SPARK_MASTER` mặc định
là `local[2]`. `HADOOP_CONF_DIR` được dùng nếu đã đặt, hoặc được tìm từ lệnh
`hdfs` như trong `build_curated.py`.

Cần có một snapshot curated hoàn chỉnh trước khi chạy báo cáo. Để cập nhật
báo cáo theo các batch streaming mới nhất đã commit, chạy lần lượt:

```bash
python scripts/build_curated.py
python scripts/build_reports.py
```

Báo cáo không tự cập nhật theo streaming. Mỗi lần chạy chọn `CURRENT.json`
một lần; nếu một tiến trình curated khác chuyển CURRENT sang snapshot mới,
lần chạy báo cáo hiện tại vẫn dùng snapshot đã chọn ban đầu.

## Kết quả

Các đường dẫn sau nằm dưới `HDFS_BASE_PATH`:

| Đường dẫn | Nội dung |
|---|---|
| `reports/CURRENT.json` | Manifest trỏ tới bộ báo cáo hiện hành |
| `reports/snapshots/<report_id>/summary.json` | Tổng quan và nguồn dữ liệu |
| `reports/snapshots/<report_id>/by_status/` | Số giao dịch và tổng tiền theo trạng thái |
| `reports/snapshots/<report_id>/by_day/` | Số giao dịch và tổng tiền theo ngày |
| `reports/snapshots/<report_id>/top_merchants/` | Top merchant theo tổng VND thành công |
| `reports/snapshots/<report_id>/manifest.json` | Manifest riêng của bộ báo cáo |

Mỗi thư mục bảng chứa một `part-*.csv` có header và `_SUCCESS`.
Các CSV có `source_snapshot_id` ở mỗi dòng. JSON chứa thêm `report_id`,
`built_at_utc` và `source_committed_batch` để truy ngược nguồn dữ liệu.
Ứng dụng đọc `reports/CURRENT.json`, sau đó dùng các đường dẫn trong `paths`.

Ví dụ đọc báo cáo trên hệ thống hiện tại:

```bash
hdfs dfs -cat /user/hdoop/credit_card_project/reports/CURRENT.json
```

Terminal in các đường dẫn cụ thể dưới tên `REPORT_SUMMARY`, `REPORT_BY_STATUS`,
`REPORT_BY_DAY`, `REPORT_TOP_MERCHANTS` và `REPORTS_CURRENT`.
Dùng `hdfs dfs -cat '<đường dẫn bảng>/part-*.csv'` để xem một bảng.

## Quy tắc tính

- `transaction_count` gồm mọi giao dịch đã có ID trong snapshot curated.
  Các cột `success_count`, `rejected_fraud_count`, `rejected_error_count`,
  `invalid_flag_count`, `invalid_data_count` là số giao dịch theo trạng thái.
- `success_amount_usd` và `success_amount_vnd` chỉ cộng giao dịch `SUCCESS`,
  dùng số tiền đã lưu trong curated và phép tính Decimal. Script không tải
  tỷ giá mới. Giao dịch hoàn tiền có số tiền âm được cộng theo đúng dấu.
- Bảng `by_status` luôn có đủ năm trạng thái, kể cả trạng thái có số lượng 0.
- Ngày lấy từ `date_formatted` của giao dịch gốc, xuất thành `yyyy-MM-dd` và
  sắp xếp theo thời gian. Đây không phải ngày chạy báo cáo hay `pos_event_time`.
  Ngày thiếu được giữ trong nhóm `UNKNOWN_DATE`; số dòng này cũng xuất hiện
  trong `summary.json` dưới tên `unknown_date_transactions`.
- Merchant được bỏ khoảng trắng ở hai đầu; merchant trống/thiếu vào nhóm
  `UNKNOWN_MERCHANT`. Gộp theo merchant qua tất cả thành phố.
- Top merchant chỉ xét merchant có ít nhất một giao dịch `SUCCESS`, sắp xếp
  theo tổng VND giảm dần, rồi tổng USD giảm dần, rồi merchant tăng dần để xử lý
  trường hợp bằng nhau. Số giao dịch trên mỗi dòng vẫn gồm mọi trạng thái
  của merchant đó. Bảng top chỉ gồm một phần các merchant, nên tổng của bảng
  này có thể khác tổng toàn bộ dữ liệu.
- Script đối chiếu số giao dịch, số lượng theo trạng thái và tổng tiền với
  manifest curated. ID trùng/thiếu, trạng thái lạ, số tiền SUCCESS thiếu hoặc
  kết quả đối chiếu sai sẽ làm lần chạy thất bại trước khi xuất báo cáo mới.

Với snapshot đã kiểm thử gồm 80 dòng nguồn, 26 dòng thiếu ID và 2 dòng trùng,
kết quả mong đợi là:

```text
TRANSACTION_COUNT: 52
SOURCE_ROWS: 80
MISSING_ID_ROWS: 26
DUPLICATE_EXTRA_ROWS: 2
SUCCESS_COUNT: 49
SUCCESS_AMOUNT_USD: 3865.38
SUCCESS_AMOUNT_VND: 99108343.20
REJECTED_ERROR: 3
```

Mỗi lần chạy tạo một bộ báo cáo mới; các bộ trước được giữ lại. Script ghi
đầy đủ vào thư mục staging, chuyển thành snapshot hoàn chỉnh rồi mới thay
`reports/CURRENT.json` bằng thao tác rename-overwrite của HDFS. Nếu thất bại,
CURRENT trước đó vẫn dùng được. Khóa file cục bộ chỉ cho một tiến trình báo cáo
chạy trên cùng máy WSL, tương tự `build_curated.py`.

## Kiểm thử

```bash
python -m unittest discover -s tests -v
```

Các bài kiểm thử dùng Spark local và API Hadoop trên filesystem tạm, không
cần Kafka hoặc HDFS. Chúng kiểm tra tổng tiền và các bảng, chạy lại, thay đổi
curated CURRENT giữa chừng, dữ liệu không hợp lệ, hoàn tiền, bộ dữ liệu không
có giao dịch thành công và lỗi lúc công bố báo cáo. Việc chạy trên HDFS thật
cần được thực hiện trong môi trường WSL của dự án.
