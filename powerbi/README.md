# Power BI Desktop: CSV từ snapshot HDFS

Bắt đầu ở đây để tạo `credit_card_dashboard.pbix` trên Windows. Bộ này gồm
script xuất dữ liệu đã loại trùng, Power Query, DAX và hướng dẫn ba trang báo cáo.
Không cần cài thêm thư viện Python hay khởi chạy Spark để export.

## 1. Xuất dữ liệu trong WSL, user hdoop

Tại môi trường Hadoop đã kiểm tra:

```bash
cd /home/hdoop/credit_card_project
source .venv/bin/activate
python scripts/export_powerbi.py
```

Mặc định lấy `HDFS_BASE_PATH` từ biến môi trường hoặc `.env`, xuất vào
`powerbi/data`. Muốn chọn vùng dữ liệu và thư mục khác:

```bash
python scripts/export_powerbi.py \
  --base /user/hdoop/credit_card_project_tests/credit-card-e2e-9xs0f453 \
  --output-dir powerbi/data
```

Đường dẫn test trên là bộ 10 giao dịch đã chạy trong phiên kiểm thử của dự án;
chỉ dùng nếu dữ liệu HDFS đó còn tồn tại. Dùng lệnh mặc định để chuyển sang dữ
liệu chính. Không cần chỉnh `.env` để đổi nguồn cho một lần xuất.

Script in `POWERBI_WINDOWS_FOLDER`: sao chép giá trị này cho tham số DataFolder.
Với WSL Ubuntu của dự án, đường dẫn mặc định là:

```text
\\wsl.localhost\Ubuntu\home\hdoop\credit_card_project\powerbi\data
```

Có thể mở đường dẫn này trong File Explorer để kiểm tra Windows nhìn thấy
CURRENT.json. Nếu distro/tên user/thư mục khác, dùng đúng đường dẫn script in ra.

## 2. Cách công bố và lưu nguồn

```text
powerbi/data/
  CURRENT.json                         ← điểm vào cố định cho Power Query
  snapshots/<export_id>/
    transactions.csv                   ← một CSV của snapshot đã loại trùng
    manifest.json                      ← nguồn HDFS, số lượng, tổng tiền, SHA-256
```

Exporter đọc curated/CURRENT.json đúng một lần, so khớp manifest riêng và
_SUCCESS, kiểm tra ID/trạng thái, phép quy đổi từng SUCCESS, số lượng và tổng
tiền. Script giữ các giao dịch lỗi/fraud và ngày trống, không tải tỷ giá mới.
Ngày giờ và tiền lấy từ dữ liệu đã lưu, không thay bằng ngày chạy export.

CSV và metadata được ghi xong trong staging trước; cuối cùng mới thay
CURRENT.json bằng thao tác nguyên tử. Nếu đọc/kiểm tra/ghi/publish lỗi, CURRENT
cũ vẫn dùng được. Bộ xuất cũ không bị sửa/xóa. Một snapshot hoàn chỉnh nhưng
chưa được CURRENT chọn sau lỗi publish có thể còn lại; Power Query không nạp nó.
Đây là lý do dùng CURRENT thay vì ghi đè hai file đang được đọc.

CURRENT chứa nguồn snapshot, batch nguồn, số dòng nguồn, số dòng loại/trùng,
số giao dịch, số theo trạng thái, tổng SUCCESS USD/VNĐ và SHA-256 của CSV.
Các bản ghi CSV có source_snapshot_id để kiểm tra nguồn trong Power Query.
`powerbi/data/` được bỏ qua trong Git. Không dùng **Folder > Combine** trên
thư mục snapshots vì sẽ cộng nhiều phiên bản của cùng giao dịch.

## 3. Tạo Power Query

1. Mở Power BI Desktop, tạo báo cáo mới.
2. Home > Get data > Blank query để mở Power Query Editor.
3. Đổi tên query thành **DataFolder**. Mở Advanced Editor, thay nội dung bằng
   `powerquery/DataFolder.pq`, chỉnh giá trị thành POWERBI_WINDOWS_FOLDER của bạn.
   Đây là tham số text cho thư mục gốc, không phải đường dẫn một CSV cụ thể.
4. Tạo Blank query thứ hai, đặt tên chính xác **Transactions**.
5. Dán toàn bộ `powerquery/Transactions.pq` vào Advanced Editor.
6. Kiểm tra preview rồi Close & Apply. Chỉ Transactions được nạp vào model.

Query đọc CURRENT một lần trong mỗi lần đánh giá, chọn đúng CSV bất biến,
kiểm tra số dòng và snapshot ID. ID/User/Card là text để giữ số 0 ở đầu.
Tiền parse với culture en-US vì CSV dùng dấu chấm thập phân, không có phân
cách hàng nghìn. Ngày parse tường minh dd/MM/yyyy; giờ HH:mm:ss.

Query giữ cả chuỗi ngày/giờ gốc và tạo **TransactionDate**, **TransactionTime**.
Ngày/giờ không đọc được trở thành null, không loại bản ghi khỏi Transactions.
Merchant/city trống có nhãn UNKNOWN_MERCHANT/UNKNOWN_CITY để visual dễ đọc.

Tiền dùng Fixed decimal/Currency.Type. Exporter từ chối giá trị không hữu hạn,
quá số lẻ hoặc ngoài phạm vi kiểu này để tránh mất độ chính xác khi nạp.
Giá trị và tổng tiền được giới hạn đối xứng ở 922337203685477.5807; FX giữ
4 chữ số lẻ, USD/VNĐ giữ 2. Đây là giới hạn của bộ xuất Power BI, không sửa
hoặc giới hạn dữ liệu lịch sử trong Hadoop. Không bật auto-detect để đổi ID
sang số hoặc thay các trường tiền thành kiểu số nguyên.

## 4. Tạo Calendar và quan hệ

1. Modeling > New table, dán toàn bộ `dax/Calendar.dax`.
2. Table tools > Mark as date table, chọn Calendar[Date].
3. Trong Model view tạo quan hệ:
   **Calendar[Date] (1) → Transactions[TransactionDate] (*)**.
   Quan hệ active, cross-filter **Single** từ Calendar sang Transactions.
4. Chọn Calendar[YearMonth], Sort by column > YearMonthSort.
5. Chọn Calendar[MonthName], Sort by column > MonthNumber.
6. Tạo hierarchy tùy chỉnh: Year → YearMonth → Date.

Calendar lấy phạm vi từ ngày giao dịch, không từ TODAY(). Với dữ liệu gốc,
các năm có thể là 2002–2020; với bộ mẫu là 2019–2020. Nếu tất cả ngày đều
trống, Calendar rỗng; chưa cần đánh dấu date table/vẽ chart thời gian cho đến
khi có ngày hợp lệ. Transactions và các chỉ số tổng quan vẫn được giữ.

## 5. Tạo measure và định dạng

Mở `dax/measures.dax`. Với **từng block tên measure**, chọn Modeling > New
measure và dán block đó; không dán cả file vào một measure hoặc DAX Query View.
Tạo theo thứ tự trong file vì measure sau dùng measure trước.

| Nhóm | Measure |
|---|---|
| Số lượng | Total Transactions, Successful Transactions, Unsuccessful Transactions |
| Phân loại lỗi | Fraud Transactions, Rejected Error Transactions, Invalid Transactions |
| Tiền | Success Amount USD, Success Amount VND |
| Tỷ lệ | Fraud Rate, Success Rate |
| Ngày | Dated Transactions, Undated Transactions |

Định dạng số lượng là Whole number; tiền là Fixed decimal hiển thị 2 chữ số
lẻ; tỷ lệ là Percentage hiển thị 2 chữ số lẻ. Đặt display units của card tiền
là None khi đối soát để thấy toàn bộ giá trị thay vì 1.04M.

Tiền chỉ cộng SUCCESS, giữ dấu âm của hoàn tiền và tính SUCCESS có số tiền 0.
Rejected Error Transactions chỉ đếm REJECTED_ERROR; Invalid Transactions gồm
INVALID_FLAG + INVALID_DATA. Không gộp fraud vào lỗi dữ liệu.

KEEPFILTERS giúp measure tôn trọng slicer trạng thái: chọn fraud thì số
SUCCESS/tiền SUCCESS bằng 0. Fraud Rate là tỷ lệ trong ngữ cảnh lọc hiện tại;
chỉ chọn fraud thì tỷ lệ 100% là đúng. Undated Transactions bỏ lọc Calendar
để vẫn thể hiện các giao dịch không thể thuộc tháng/năm đang chọn; vẫn giữ
lọc merchant/city/trạng thái.

Dùng các measure đã cung cấp khi vẽ tiền, không kéo cột amount_vnd để tạo một
implicit SUM thiếu điều kiện SUCCESS. Không SUM fx_rate. User/Card là text;
Card trong CSV có thể là chỉ số thẻ trong một user, nên khi phân tích thẻ cần
xem cặp User và Card thay vì coi Card riêng lẻ là ID toàn cục.

## 6. Trang Tổng quan

Đặt tên trang **Tổng quan**, canvas 16:9.

- Hàng đầu: card Total Transactions, Successful Transactions, Success Amount VND,
  Fraud Rate. Đặt nhãn tiếng Việt tương ứng Tổng giao dịch / Thành công /
  Tổng VNĐ thành công / Tỷ lệ fraud.
- Hàng tiếp: card Fraud Transactions, Rejected Error Transactions,
  Invalid Transactions và Undated Transactions.
- Clustered column chart: trục Transactions[final_status], giá trị Total Transactions.
- Table: final_status, Total Transactions, Success Amount USD, Success Amount VND.
- Slicer: merchant, city, final_status. Chưa đặt slicer Calendar trên trang này
  để các giao dịch chưa rõ ngày vẫn hiện trong tổng quan mặc định.

## 7. Trang Merchant

- Bar chart: trục Transactions[merchant], giá trị Success Amount VND; sort giảm dần.
  Khi nhiều merchant, dùng Top N = 10 ở riêng visual này, theo Success Amount VND.
- Matrix: merchant; các giá trị Total Transactions, Successful Transactions,
  Fraud Transactions, Rejected Error Transactions, Invalid Transactions,
  Success Amount USD và Success Amount VND. Matrix không bị giới hạn Top N.
- Slicer: merchant, city, final_status.

Total Transactions và Successful Transactions là hai chỉ số khác nhau. Không
cộng riêng top 10 rồi so với tổng toàn bộ dữ liệu.

## 8. Trang Thời gian

- Line chart: trục Calendar[Date], giá trị Success Amount VND.
- Column chart: trục Calendar[YearMonth], giá trị Dated Transactions.
- Matrix theo hierarchy Year → YearMonth → Date; giá trị Dated Transactions,
  Successful Transactions, Success Amount USD/VND. Dùng drill để xem ngày/tháng/năm.
- Card Undated Transactions để giữ khả năng đối soát ngày trống.
- Slicer Calendar[Date] hoặc Year/YearMonth, merchant và final_status.
- Với các visual có trục Calendar, lọc Calendar[Date] **is not blank** để không
  xuất hiện nhóm ngày trống. Không áp bộ lọc đó lên toàn báo cáo.

Với bộ mẫu: tổng quan 10 giao dịch; 9 có ngày hợp lệ và 1 chưa rõ ngày.
Khoản không rõ ngày vẫn nằm trong bảng Transactions và tổng quan.

## 9. Đối soát bằng bộ đã kiểm thử

Khi export từ vùng HDFS test trong bước 1 và chưa có thêm giao dịch mới:

| Chỉ số | Kỳ vọng |
|---|---:|
| Total Transactions | 10 |
| Successful Transactions | 5 |
| Fraud Transactions | 1 |
| Rejected Error Transactions | 1 |
| Invalid Transactions | 3 |
| Unsuccessful Transactions | 5 |
| Success Amount USD | 40.50 |
| Success Amount VND | 1038420.00 |
| Fraud Rate / Success Rate | 10% / 50% |
| Dated / Undated Transactions | 9 / 1 |

Matrix merchant: TEST_MERCHANT_A có 3 SUCCESS, 28 USD, 717920 VNĐ;
TEST_MERCHANT_B có 2 SUCCESS, 12.50 USD, 320500 VNĐ.
Một bản gửi lặp làm source_rows=11 và duplicate_extra_rows=1 trong metadata,
nhưng CSV/Power BI vẫn có 10 giao dịch và tổng tiền không đổi.
Các kỳ ngày/tháng/năm phải khớp các bảng merchant_by_* trong reports snapshot
có cùng source_snapshot_id, không so với một báo cáo HDFS đã cũ.

## 10. Refresh và lưu PBIX

1. Lưu báo cáo thành `powerbi/credit_card_dashboard.pbix` bằng Save As trong Desktop.
2. Khi có dữ liệu mới đã commit, chạy trong WSL:

```bash
python scripts/build_curated.py
python scripts/build_reports.py
python scripts/export_powerbi.py
```

3. Trở lại Desktop, Home > Refresh. Giữ nguyên DataFolder; Power Query tự đọc
   CURRENT mới, không cần chọn lại file CSV hoặc sửa query.
4. Export lại cùng curated snapshot sẽ tạo export_id mới nhưng cùng dữ liệu;
   Refresh không làm tăng số lượng giao dịch.
5. Chuyển môi trường bằng --base khi export hoặc thay DataFolder nếu đổi thư
   mục đầu ra. Không trộn các nguồn vào chung một Transactions.

Cả thư mục gốc export và snapshots bên trong cần đi cùng nhau khi sao chép
sang ổ Windows khác. Không xóa snapshot cũ khi Desktop đang refresh.
Các script không tự điều khiển giao diện Desktop hoặc tạo file PBIX nhị phân.
Bạn dựng/lưu PBIX bằng các bước trên; Power Query/DAX cần được xác nhận trong
Desktop bằng các giá trị nghiệm thu, không coi kiểm thử Python là chạy DAX.

## 11. Kiểm thử và tự động hóa sau này

```bash
python -m unittest discover -s tests -p test_export_powerbi.py -v
```

Test không cần HDFS/Spark: dữ liệu hợp lệ, hoàn tiền, ID text, số lượng/tổng
sai, snapshot CURRENT đổi giữa chừng, lỗi công bố, CSV cũ/mới và gửi lặp.
Tài liệu nghiệm thu môi trường thật nằm trong docs/integration-testing.md.

Airflow và Power BI Service/gateway chưa được triển khai ở chặng này. Khi đã
xác minh tài khoản/quyền phù hợp, chốt lịch và chuỗi phụ thuộc:
`tỷ giá → curated → reports → export → refresh semantic model`.
Không coi CSV tự cập nhật là Power BI Desktop tự refresh.

## Nguồn kỹ thuật

- CSV giữ UTF-8, dấu nháy và xuống dòng theo [Csv.Document](https://learn.microsoft.com/en-us/powerquery-m/csv-document).
- Parse ngày có format/culture tường minh theo [Date.FromText](https://learn.microsoft.com/en-us/powerquery-m/date-fromtext).
- Giới hạn và độ chính xác theo [Currency.From](https://learn.microsoft.com/en-us/powerquery-m/currency-from).
- Bảng ngày liên tục theo [CALENDAR](https://learn.microsoft.com/en-us/dax/calendar-function-dax).
- Slicer giao với điều kiện SUCCESS theo [KEEPFILTERS](https://learn.microsoft.com/en-us/dax/keepfilters-function-dax).
- Connector Hadoop on-premises có giới hạn scheduled refresh theo [Hadoop File](https://learn.microsoft.com/en-us/power-query/connectors/hadoop-file-system).
