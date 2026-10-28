# Chạy POS và Kafka dispatcher bằng một lệnh

Tại thư mục gốc dự án, kích hoạt `.venv` đang dùng và chạy:

```bash
python run_pos.py
```

Lệnh này chạy hai tiến trình với cùng Python của môi trường hiện tại:

- POS tiếp tục từ `NEXT_CSV_LINE` trong SQLite, mặc định tạo một giao dịch mỗi 3 giây.
- Dispatcher dùng `--follow`, nghỉ ngẫu nhiên 1–5 giây trước mỗi lần gửi Kafka, chỉ đánh dấu `SENT` sau ACK.

Không cần `--limit` khi chạy bình thường. Nhấn **Ctrl+C** một lần để dừng cả hai.
Chạy lại cùng lệnh sẽ tiếp tục từ tiến độ đã commit; các dòng `PENDING` dùng lại ID và payload đã lưu.
Nếu Kafka chưa chạy, POS vẫn ghi vào SQLite và dispatcher thử kết nối lại sau mỗi 5 giây.

CSV là nguồn hữu hạn và được kiểm tra SHA-256. Khi hết CSV, POS dừng tạo mới;
launcher chờ gửi hết `PENDING` rồi kết thúc. Nếu broker đang tắt, nó tiếp tục chờ
cho đến khi broker hoạt động hoặc người dùng nhấn Ctrl+C. Không tự đọc lại từ đầu
và gán ID mới cho các dòng đã xử lý. Không sửa CSV, xóa SQLite hoặc đặt lại cursor
để chạy lại bài thử trên nguồn hiện tại.

Thử nhanh tối đa 3 giao dịch **mới**, đồng thời gửi các giao dịch đang `PENDING`:

```bash
python run_pos.py --limit 3
```

Đổi nhịp tạo giao dịch POS (không đổi khoảng nghỉ ngẫu nhiên của dispatcher):

```bash
python run_pos.py --interval 2
```

Kiểm tra trạng thái mà không tạo hoặc gửi giao dịch:

```bash
python producer/pos_simulator.py --status
```

Cũng có thể chạy riêng POS. Mặc định đọc mọi dòng còn lại; `--limit` chỉ giới hạn
số giao dịch mới khi muốn thử ít dòng:

```bash
python producer/pos_simulator.py
python producer/pos_simulator.py --limit 3
```

Giữ Kafka, HDFS và Spark Streaming theo cách chạy hiện tại. `run_pos.py` chỉ
quản lý POS và dispatcher. Dừng các bản POS/dispatcher cũ trước khi dùng launcher;
mỗi thành phần có khóa theo outbox để tránh hai tiến trình cùng làm một việc.

## Kiểm thử local

Cần các dependency hiện có `python-dotenv` và `kafka-python`:

```bash
python -m unittest discover -s tests -p 'test_pos_runner.py' -v
```

Các bài thử dùng SQLite tạm, Kafka giả lập và tiến trình con thật để kiểm tra
tiếp tục cursor, ACK/lỗi gửi, broker bật muộn, dừng cả hai bằng Ctrl+C, và dọn
tiến trình còn lại khi một tiến trình lỗi. Không gửi dữ liệu tới Kafka thật.
