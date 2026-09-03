# Thiết kế: Tra cứu địa chỉ hành chính Việt Nam từ toạ độ GPS trong Spark SQL

- **Ngày:** 03/09/2026
- **Trạng thái:** Đã duyệt thiết kế, chờ lập kế hoạch triển khai
- **Nguồn dữ liệu:** [thanglequoc/vietnamese-provinces-database](https://github.com/thanglequoc/vietnamese-provinces-database)

---

## 1. Mục tiêu và phạm vi

### 1.1 Bài toán

Datalake hiện có nhiều bảng chứa toạ độ GPS (`lat`, `lon`) nhưng không có thông tin địa chỉ. Cần một thành phần dùng chung để bất kỳ job Spark SQL nào cũng gắn được địa chỉ hành chính vào toạ độ.

### 1.2 Phạm vi

**Trong phạm vi.** Từ một cặp `(lat, lon)` trả về đơn vị hành chính chứa điểm đó: phường/xã/đặc khu, tỉnh/thành phố, mã đơn vị, mã bưu chính — theo mô hình hành chính hai cấp có hiệu lực tại thời điểm dữ liệu phát sinh.

**Ngoài phạm vi.** Tên đường và số nhà. Nguồn dữ liệu không chứa thông tin này; muốn có phải dùng OpenStreetMap hoặc dịch vụ thương mại. Người dùng đã xác nhận không cần và không muốn gọi API ngoài.

**Ngoài phạm vi.** Chiều ngược lại (từ địa chỉ văn bản ra toạ độ). Bảng chiều thiết kế ở đây có thể phục vụ một phần cho việc đó về sau, nhưng không phải mục tiêu hiện tại.

### 1.3 Ràng buộc

| Ràng buộc | Nội dung |
|---|---|
| Hạ tầng | PySpark trên YARN, điều phối bằng Airflow, kho bảng Iceberg — theo khuôn mẫu datalake hiện có |
| Kết nối mạng | Không phụ thuộc dịch vụ ngoài lúc chạy; toàn bộ dữ liệu nằm trong cụm |
| Phụ thuộc | Được phép cài jar Java trên cụm (Apache Sedona) |
| Hình thái | Thành phần dùng chung cho nhiều job, không phải một pipeline riêng |
| Ngữ nghĩa thời gian | SCD2 — địa chỉ theo mô hình hành chính tại thời điểm dữ liệu phát sinh |

---

## 2. Ràng buộc thời gian của dữ liệu nguồn

Đây là ràng buộc quan trọng nhất của toàn bộ thiết kế và cần được hiểu đúng trước mọi thứ khác.

### 2.1 Dữ liệu ranh giới chỉ có từ tháng 6/2026

| Phiên bản | Ngày phát hành | Có ranh giới GIS | Cấu trúc hành chính |
|---|---|---|---|
| v1.0.1 → v2.4.0 | 06/2022 → 03/2025 | Không | 63 tỉnh, ba cấp (có quận/huyện) |
| v3.0.2 | 20/07/2025 | Không | 34 tỉnh, hai cấp (sau sáp nhập) |
| v3.1.0 | 23/05/2026 | Không | Thêm việc lập TP. Đồng Nai |
| v4.0.0 | 21/06/2026 | **Có** | 34 tỉnh, hai cấp |
| v4.1.0 | 01/08/2026 | Có | Thêm GeoJSON, vá GIS Hoàng Sa và Trường Sa |
| v4.2.0 | 02/08/2026 | Có | Thêm ElasticSearch và GIS cho MongoDB |
| v5.0.0 | 31/08/2026 | Có | Thêm mã bưu chính |

Hệ quả: **không tồn tại ranh giới hành chính của giai đoạn trước tháng 6/2026 trong nguồn này.** Với một điểm GPS phát sinh trước ngày Nghị quyết 202/2025/QH15 có hiệu lực, không có cách nào suy ra phường/xã đương thời từ toạ độ — các phường/xã cũ nhỏ hơn và đã được gộp theo cách không đảo ngược được bằng dữ liệu hiện có.

### 2.2 Chính sách đã chốt

1. Bảng chiều dựng theo đúng khung SCD2 (`valid_from`, `valid_to`, `is_current`, `decree_ref`). Bản ghi đầu tiên là ảnh chụp hiện hành, `valid_from` gán theo **ngày hiệu lực của nghị quyết tương ứng**, không phải ngày phát hành repo. Khung này hoạt động đúng ngay từ nghị quyết kế tiếp trở đi.
2. Dữ liệu phát sinh trước mốc ranh giới sớm nhất vẫn được gắn địa chỉ, theo ranh giới sớm nhất hiện có, kèm cột cờ `geo_ref_is_exact = false`. Kết quả khi đó là **quy chiếu về mô hình hành chính mới**, không phải địa chỉ đương thời. Nói rõ điều này tốt hơn im lặng trả về một giá trị trông có vẻ đúng.
3. Khoảng 01/07/2025 → 29/04/2026 về lý thuyết vá được (khác biệt duy nhất so với hiện hành là việc lập TP. Đồng Nai), nhưng **không làm trong phạm vi này**. Công không nhỏ, ảnh hưởng một tỉnh, và khung SCD2 cho phép bổ sung sau mà không phải làm lại gì.

---

## 3. Lựa chọn phương án

Ba hướng đã cân nhắc, tất cả đều dùng chung một bảng chiều sinh ra từ repo; khác nhau ở việc Spark làm gì lúc truy vấn.

| Hướng | Cách làm | Vì sao không chọn |
|---|---|---|
| UDF vô hướng `vn_reverse_geocode(lat, lon)` | Pandas UDF, mỗi executor dựng cây chỉ mục không gian từ dữ liệu broadcast | Cập nhật dữ liệu kéo theo triển khai lại venv; ngữ nghĩa theo thời điểm phải nhét vào tham số hàm và giữ nhiều phiên bản trong bộ nhớ executor; phải broadcast 150–250 MB mỗi lần chạy job |
| **Ghép không gian bằng Sedona** | `ST_Contains` chạy trong JVM, bảng chiều là bảng Iceberg thường | **Đã chọn** |
| Bảng tra cứu lưới H3 | Tiền tính ô lưới → mã xã, lúc chạy chỉ là JOIN băm | Sai số vùng giáp ranh khoảng 100 m ở độ phân giải 9; công tiền tính lớn; vẫn cần một UDF để mã hoá ô lưới. Là hướng tối ưu cho quy mô hàng tỷ dòng, chưa cần thiết |

### 3.1 Vì sao chọn Sedona

Quyết định ban đầu nghiêng về UDF vô hướng vì khớp với yêu cầu "hàm dùng chung". Yêu cầu **dữ liệu cập nhật thường xuyên** đã đảo ngược lựa chọn đó, vì ba lý do:

1. **Cập nhật dữ liệu không kéo theo triển khai code.** Bảng chiều là bảng Iceberg thường; ghi dữ liệu mới xong là câu SQL hôm sau dùng bản mới, không đụng đến venv hay HDFS.
2. **Ngữ nghĩa theo thời điểm là chuyện tự nhiên.** SCD2 chỉ là một vế điều kiện thêm vào JOIN. Với UDF thì phải thêm tham số ngày và giữ nhiều phiên bản cây chỉ mục trong bộ nhớ.
3. **Không phải broadcast lại mỗi lần chạy.** Vì dữ liệu đổi thường xuyên nên không thể nướng sẵn geometry vào artifact.

Lợi ích kèm theo: geometry nằm trong bảng chứ không broadcast, nên **giữ nguyên độ phân giải đầy đủ** của polygon thay vì phải rút gọn — cho độ chính xác tuyệt đối ở vùng giáp ranh.

Cái giá phải trả: người dùng viết JOIN thay vì gọi hàm. Bù lại bằng một hàm Python bọc sẵn câu JOIN (mục 5.2).

---

## 4. Kiến trúc

Hệ thống tách làm hai nửa, khác nhau về nơi chạy và tần suất chạy.

```
NỬA A — Dựng dữ liệu (ngoài cụm, 3–4 lần một năm)

  GitHub release (ghim theo tag, vd v5.0.0)
    │
    ├─ json/vn_provinces_wards_geojson.zip  (47,6 MB) ──┐
    └─ json/full_json_generated_data_vn_units.json ─────┤
                                                        ▼
                          ghép + gắn cột SCD2 → kiểm định → Parquet
                                                        │
NỬA B — Nạp và phục vụ (trên cụm)                       ▼
                          job PySpark MERGE vào Iceberg dim.dim_vn_ward_gis
                                                        │
                                   ┌────────────────────┴────────────────────┐
                                   ▼                                         ▼
                     view dim_vn_ward_gis_current              module vn_geo.attach_vn_address()
                                   └──────────── mọi job Spark SQL ──────────┘
```

### 4.1 Nguồn đọc

Đọc **GeoJSON zip + JSON thuộc tính** thay vì dump PostGIS (145 MB, bốn phần). Dump PostGIS đòi dựng một Postgres chỉ để trung chuyển dữ liệu — thêm một mắt xích hạ tầng cho một việc chạy vài lần một năm. File zip 47,6 MB đọc thẳng bằng Python, không phụ thuộc gì.

### 4.2 Các thành phần

| Thành phần | Trách nhiệm | Nơi chạy | Phụ thuộc |
|---|---|---|---|
| `source_fetcher` | Tải release theo tag đã ghim, đối chiếu checksum | Ngoài cụm | `requests` |
| `geojson_reader` | Zip → luồng `(ward_code, geometry)` | Ngoài cụm | thư viện chuẩn, `shapely` |
| `attribute_reader` | JSON → thuộc tính hành chính, loại đơn vị, mã bưu chính | Ngoài cụm | thư viện chuẩn |
| `dim_builder` | Ghép hai nguồn, sinh WKB và bbox, gắn cột SCD2 | Ngoài cụm | `shapely`, `pyarrow` |
| `validator` | Kiểm định độc lập trên kết quả builder | Ngoài cụm | `shapely` |
| `iceberg_loader` | MERGE theo ngữ nghĩa SCD2 vào bảng đích | Trên cụm | PySpark, Iceberg |
| `vn_geo` | API cho job tiêu thụ: bọc câu JOIN không gian | Trên cụm | PySpark, Sedona |

Mỗi thành phần có một trách nhiệm và kiểm thử được độc lập. Chỉ `vn_geo` nằm trên đường chạy nóng hằng ngày; sáu thành phần còn lại chạy vài lần một năm. Ranh giới này giữ cho phần thường trực trong datalake mỏng nhất có thể — đúng một module đọc bảng và ghép JOIN.

### 4.3 Điểm cần xác minh trước khi code

**Tương thích phiên bản Sedona.** Phiên bản Sedona phải khớp với phiên bản Spark và Scala của cụm. Đây là việc phải hỏi đội hạ tầng và xác nhận bằng một job thử chạy được, **trước** khi bắt đầu hiện thực. Nếu không khớp, toàn bộ hướng 2 phải xem lại và hướng UDF vô hướng là phương án dự phòng.

---

## 5. Lược đồ và hợp đồng giao diện

### 5.1 Bảng `dim.dim_vn_ward_gis` (Iceberg)

Khoá nghiệp vụ: `(ward_code, valid_from)`. Mỗi phiên bản 3321 dòng. Không phân vùng — bảng quá nhỏ để phân vùng có ý nghĩa. Sắp xếp theo `province_code` để gom polygon lân cận vào cùng file.

| Nhóm | Cột | Kiểu | Ghi chú |
|---|---|---|---|
| Xã/phường | `ward_code` | STRING | Mã đơn vị hành chính |
| | `ward_name` | STRING | vd "Bến Nghé" |
| | `ward_full_name` | STRING | vd "Phường Bến Nghé" |
| | `ward_name_en`, `ward_full_name_en` | STRING | Bản tiếng Anh |
| | `ward_code_name` | STRING | Slug không dấu, dùng đối chiếu chuỗi |
| | `ward_type` | STRING | Phường / Xã / Đặc khu |
| Bưu chính | `postal_code` | STRING | |
| Tỉnh/thành | `province_code` | STRING | |
| | `province_name`, `province_full_name`, `province_name_en` | STRING | |
| | `province_postal_code_prefix` | STRING | |
| Không gian | `bbox` | BINARY | WKB Polygon, dùng lọc thô |
| | `geom` | BINARY | WKB MultiPolygon, EPSG:4326, nguyên độ phân giải |
| | `area_km2` | DECIMAL(12,5) | |
| SCD2 | `valid_from` | DATE | Ngày hiệu lực nghị quyết |
| | `valid_to` | DATE | Cận trên mở; `9999-12-31` cho bản hiện hành |
| | `is_current` | BOOLEAN | |
| | `decree_ref` | STRING | vd "30/2026/QH16" |
| Truy vết | `source_release` | STRING | vd "v5.0.0" |
| | `ingested_at` | TIMESTAMP | |

`ward_type` tách riêng vì nhiều báo cáo cần lọc theo loại đơn vị mà không phải bóc chuỗi từ `ward_full_name`.

Kèm theo: view `dim.dim_vn_ward_gis_current` định nghĩa là `SELECT * FROM dim.dim_vn_ward_gis WHERE is_current`.

Tên catalog và schema cụ thể bám theo quy ước đặt tên của datalake hiện có; chốt lúc lập kế hoạch triển khai.

### 5.2 Hai đường vào, một ngữ nghĩa

**Đường SQL trực tiếp:**

```sql
SELECT t.*, w.ward_full_name, w.province_full_name, w.postal_code
FROM   my_table t
LEFT JOIN dim.dim_vn_ward_gis w
  ON  ST_Contains(ST_GeomFromWKB(w.geom), ST_Point(CAST(t.lon AS DOUBLE), CAST(t.lat AS DOUBLE)))
  AND greatest(t.txn_date, DATE '2026-04-30') >= w.valid_from
  AND greatest(t.txn_date, DATE '2026-04-30') <  w.valid_to
```

Hai lưu ý khi viết tay:

- `ST_Point(x, y)` nhận **kinh độ trước, vĩ độ sau**. Đây là nguồn lỗi phổ biến nhất.
- Hằng ngày `DATE '2026-04-30'` ở trên là **`valid_from` nhỏ nhất đang có trong bảng**, viết thẳng ra cho dễ đọc. Trong hàm bọc, giá trị này được đọc từ bảng chứ không gõ cứng, để khi bổ sung phiên bản ranh giới cũ hơn (mục 9.3) thì hành vi tự đúng theo. Ai viết SQL tay cần tự thay bằng giá trị hiện hành, hoặc dùng hàm bọc.

**Đường hàm bọc:**

```python
from vn_geo import attach_vn_address

df2 = attach_vn_address(
    df,
    lat_col="lat",
    lon_col="lon",
    as_of_col="txn_date",   # bỏ trống -> dùng view _current
    prefix="addr_",
)
```

Hai đường phải cho kết quả giống hệt nhau trên cùng đầu vào; đây là một bài kiểm thử bắt buộc (mục 6.2).

### 5.3 Ba quyết định thiết kế

**Luôn `LEFT JOIN`, không bao giờ `INNER`.** Một điểm GPS không tra được không được phép làm biến mất bản ghi nghiệp vụ. Số dòng vào bằng số dòng ra, luôn luôn; hàm bọc kiểm tra bất biến này.

**Khử trùng lặp tất định.** Ghép không gian có thể nhân đôi dòng khi hai polygon chồng lấn nhẹ ở ranh giới. Hàm bọc chọn `ward_code` nhỏ nhất và đánh dấu `addr_match_status = 'AMBIGUOUS'`, không im lặng chọn bừa. Người viết SQL tay được cảnh báo trong tài liệu.

**Trả về trạng thái, không chỉ trả về giá trị.** Ngoài các cột địa chỉ, hàm thêm:

| Cột | Kiểu | Giá trị |
|---|---|---|
| `addr_match_status` | STRING | `MATCHED` — rơi gọn trong đúng một xã/phường |
| | | `AMBIGUOUS` — khớp nhiều polygon, đã chọn tất định |
| | | `OUT_OF_BOUNDS` — không rơi vào polygon nào |
| | | `INVALID_INPUT` — toạ độ rỗng, bằng 0, hoặc ngoài dải hợp lệ |
| `addr_geo_ref_is_exact` | BOOLEAN | `false` khi `txn_date` sớm hơn mốc ranh giới sớm nhất |

Mệnh đề `greatest(...)` trong câu SQL chính là thứ hiện thực hoá chính sách ở mục 2.2: dữ liệu cũ được kéo về phiên bản ranh giới sớm nhất hiện có thay vì trả về rỗng.

---

## 6. Xử lý lỗi và trường hợp biên

Nguyên tắc bao trùm, bất đối xứng có chủ ý:

> **Nửa dựng dữ liệu hỏng là dừng ngay. Nửa truy vấn không bao giờ được ném lỗi.**

Một bản dim sai không được lên bảng — kiểm định thất bại là chặn xuất bản. Ngược lại, một toạ độ rác trong 50 triệu dòng không được làm sập job ETL lúc 2 giờ sáng; nó đi ra ở cột `addr_match_status`.

### 6.1 Đầu vào bẩn

Kiểm tra theo thứ tự, dừng ở lỗi đầu tiên, gắn `INVALID_INPUT`:

| Trường hợp | Cách nhận biết |
|---|---|
| Toạ độ rỗng | `NULL` ở một trong hai cột |
| "Null Island" | `lat = 0 AND lon = 0` — mặc định của thiết bị chưa bắt được tín hiệu |
| Ngoài dải toàn cầu | `lat` ngoài [-90, 90] hoặc `lon` ngoài [-180, 180] |
| Đảo ngược lat/long | `lat` ngoài [-90, 90] nhưng hoán vị lại thì hợp lệ |

Việt Nam có vĩ độ khoảng 8–23 và kinh độ khoảng 102–117, nên một cặp bị hoán vị hầu như luôn cho vĩ độ lớn hơn 90 và lộ ra ngay. **Không tự động sửa** — im lặng sửa dữ liệu người khác là nguy hiểm. Gắn `INVALID_INPUT` và ghi số đếm để đội nguồn sửa từ gốc.

### 6.2 Hộp bao quốc gia

Lọc thô bằng hộp bao trước khi so polygon. Hộp bao này **tính từ chính dữ liệu lúc dựng bảng và lưu làm siêu dữ liệu**, không gõ cứng. Lý do là Hoàng Sa và Trường Sa: một hộp bao ước lượng bằng trí nhớ rất dễ cắt mất quần đảo Trường Sa ở phía nam hoặc phía đông. Repo đã vá GIS cho hai quần đảo này ở v4.1.0 nên dữ liệu có sẵn; việc của ta là đừng loại chúng ra bằng một con số phỏng đoán.

### 6.3 Điểm không rơi vào xã/phường nào

| Nguyên nhân | Kết quả `OUT_OF_BOUNDS` là đúng hay sai |
|---|---|
| Ngoài lãnh thổ (Lào, Campuchia, Trung Quốc) | Đúng |
| Trên biển giữa các đảo Trường Sa | Đúng — polygon là đảo, không phải vùng biển |
| GPS trôi vài chục mét ra biển ở ven bờ | Sai — điểm thực chất thuộc xã ven biển |
| Rơi vào khe hở giữa hai polygon liền kề | Sai — khiếm khuyết tô-pô của dữ liệu nguồn |

Hai trường hợp cuối thường được xử lý bằng cơ chế hút về xã gần nhất trong bán kính X mét. **Chưa xây trong phạm vi này.** Thay vào đó, kiểm định đo hai con số: tổng diện tích khe hở giữa các polygon (mục 7.1), và tỉ lệ `OUT_OF_BOUNDS` trên mẫu dữ liệu thật. Nếu tỉ lệ không đáng kể thì cơ chế hút là công nợ thừa; nếu đáng kể thì lúc đó có số liệu để chọn bán kính đúng thay vì bịa. Giao diện hàm chừa sẵn tham số `snap_tolerance_m` mặc định `0`, nên bật sau không phải sửa lời gọi ở các job.

### 6.4 Hình học lỗi từ nguồn

Polygon tự cắt hoặc vòng không khép kín làm `ST_Contains` sai một cách âm thầm. Repo có bộ sửa hình học riêng nhưng không tin suông: lúc dựng bảng, mọi geometry đi qua `ST_IsValid`; cái hỏng thì sửa bằng `make_valid`, và **số lượng phải sửa được ghi vào nhật ký dựng bảng**. Con số này nhảy vọt ở một phiên bản mới là tín hiệu nguồn có vấn đề, phải xem trước khi xuất bản.

---

## 7. Kiểm thử và xác minh

### 7.1 Kiểm định bảng chiều — cửa chặn xuất bản

Chạy mỗi lần dựng; bất kỳ mục nào hỏng là không đẩy lên Iceberg.

| Kiểm định | Ngưỡng |
|---|---|
| Số tỉnh và số xã/phường | Khớp giữa nguồn GeoJSON và nguồn JSON thuộc tính. Không gõ cứng con số, để nghị quyết sau không phải sửa kiểm định |
| Toàn vẹn tham chiếu | Mọi `ward_code` có geometry đều có thuộc tính và ngược lại |
| Hình học hợp lệ | 100% hợp lệ sau bước sửa; ghi nhật ký số lượng phải sửa |
| Không polygon rỗng | Không có diện tích bằng 0 |
| Tổng diện tích | Khoảng 331.000 km², sai lệch trong ±2% |
| Chồng lấn | Tổng diện tích giao nhau giữa các cặp xã dưới 0,01% tổng diện tích |
| Khe hở | Diện tích tỉnh trừ tổng diện tích các xã thuộc tỉnh dưới 0,01% |
| Nhất quán chéo GIS ↔ thuộc tính | Điểm đại diện mỗi xã nằm trong polygon của đúng tỉnh mà bảng thuộc tính khai báo |
| Toàn vẹn SCD2 | Không có hai bản ghi cùng `ward_code` chồng lấn khoảng thời gian; đúng một bản `is_current` |

Kiểm định chạy **ngoài cụm bằng `shapely`**, không phải bằng Sedona: `geom.is_valid` để kiểm tra, `shapely.make_valid()` để sửa, `geom.representative_point()` để lấy điểm đại diện. Lý do là kiểm định phải chạy được trước khi dữ liệu lên cụm — đó mới là điểm của một cửa chặn xuất bản.

Dùng **điểm đại diện, không dùng trọng tâm** (`representative_point()`, không phải `centroid`). Trọng tâm của một xã hình vòng cung hoặc gồm nhiều mảnh rời có thể rơi ra ngoài chính nó và gây báo lỗi giả hàng loạt. Bài kiểm tra tương ứng ở mục 7.3 chạy trên cụm thì dùng `ST_PointOnSurface`.

Hai mục chồng lấn và khe hở vừa là cửa chặn, vừa là căn cứ quyết định có cần cơ chế hút về xã gần nhất (mục 6.3).

### 7.2 Kiểm thử phần mềm

Viết kiểm thử trước, theo TDD, cho từng thành phần ở mục 4.2.

- **Bộ đọc và bộ dựng** — chạy trên tập cố định 3–5 xã, so với kết quả kỳ vọng ghi cứng trong test. Không cần mạng, chạy trong một giây.
- **Bộ kiểm định** — nuôi bằng dữ liệu cố ý làm hỏng: polygon tự cắt, thiếu thuộc tính, hai khoảng thời gian chồng nhau. Mỗi luật kiểm định phải có một ca chứng minh nó thật sự bắt được lỗi. Một bộ kiểm định chưa từng thất bại trong test là một bộ kiểm định chưa được kiểm thử.
- **Hàm `vn_geo`** — chạy trên Spark local kèm Sedona:
  - Bất biến số dòng: `n` dòng vào ra đúng `n` dòng, kể cả khi có điểm khớp nhiều polygon.
  - Mỗi giá trị `addr_match_status` có ít nhất một ca kiểm thử.
  - Đường SQL tay và đường hàm bọc cho kết quả giống hệt trên cùng đầu vào.
  - Cùng một toạ độ với `txn_date` khác nhau chọn đúng phiên bản SCD2 khác nhau, và `geo_ref_is_exact` đúng ở hai phía của mốc ranh giới sớm nhất.

### 7.3 Xác minh độ chính xác thực địa

**Điểm đại diện của mỗi polygon.** 3321 ca tự động, không tốn công thu thập: `ST_PointOnSurface` của mỗi xã tra ngược phải ra đúng xã đó. Bài này chạy trên cụm, qua đúng đường mà job thật sẽ đi. Bài này không chứng minh dữ liệu nguồn đúng, nhưng bắt gọn mọi lỗi lệch chỉ mục, nhầm thứ tự lat/long, sai hệ toạ độ — nhóm lỗi nguy hiểm nhất vì chúng cho kết quả trông vẫn hợp lý.

**Toạ độ chi nhánh và ATM của ngân hàng.** Bộ điểm vàng chính. Địa chỉ đã được đối soát nghiệp vụ, và phân bố địa lý đúng bằng phân bố dữ liệu thật mà hệ thống sẽ gặp — khác hẳn một tập điểm chọn ngẫu nhiên.

**Kiểm thử hai bên ranh giới.** Vài chục cặp điểm cách đường ranh giới khoảng 10 mét về mỗi phía, kiểm tra ra hai xã khác nhau. Đây là bài đo trực tiếp lợi thế của phương án đã chọn so với hai phương án còn lại.

**Chỉ tiêu: từ 99% khớp ở cấp tỉnh, từ 97% ở cấp xã/phường trên bộ điểm vàng.** Cố ý không đặt 100%: địa chỉ hành chính đăng ký của một toà nhà đôi khi thật sự khác vị trí địa lý của nó — nhà sát ranh giới, hoặc địa chỉ ghi theo mặt tiền đường thuộc phường bên cạnh. Đặt 100% sẽ biến những ca này thành lỗi phần mềm và dẫn đến sửa cái không hỏng. Phần chênh lệch phải được xem từng ca và ghi lại kết luận, không bỏ qua.

### 7.4 Kiểm thử hiệu năng

Đo ở ba mức — 1 triệu, 10 triệu, 100 triệu điểm — ghi lại thời gian chạy kèm cấu hình `spark.sedona.join.gridtype` và số executor. Mục đích là có mốc: khi nâng phiên bản Sedona hoặc đổi cấu hình cụm, phát hiện được hồi quy hiệu năng thay vì để nó âm thầm làm chậm pipeline.

---

## 8. Vận hành khi có nghị quyết mới

Nhịp thực tế theo lịch sử phát hành: 2022 có 3 bản, 2023 có 1, 2024 có 4, 2025 có 3, 2026 có 4 — khoảng **3–4 lần một năm**.

### 8.1 Quy trình sáu bước

1. **Phát hiện.** Kiểm tra release mới theo quý. Không dựng sensor tự động: cụm thường không ra được internet, và mỗi quý một lần thì một dòng trong lịch vận hành rẻ hơn một dịch vụ phải nuôi.
2. **Xác định ngày hiệu lực.** Đọc ghi chú phát hành để lấy số hiệu nghị quyết và ngày hiệu lực. `valid_from` phải là **ngày nghị quyết có hiệu lực, không phải ngày repo phát hành** — v3.0.2 phát hành 20/07/2025 cho một nghị quyết có hiệu lực từ 01/07/2025. Lấy nhầm mốc thì mọi giao dịch trong khoảng lệch bị gắn sai phiên bản hành chính, và không ai phát hiện cho tới lúc đối soát.
3. **Dựng và kiểm định.** Chạy nửa A với tag mới. Đầu ra: Parquet mới, báo cáo kiểm định (mục 7.1), và **báo cáo khác biệt** so với phiên bản hiện hành — xã nào thêm, xoá, đổi tên, đổi ranh giới.
4. **Người duyệt.** Báo cáo khác biệt phải có người đọc và chấp thuận. Cửa người có chủ ý: nếu một nghị quyết lẽ ra chỉ đổi một tỉnh mà báo cáo cho thấy 900 xã đổi ranh giới, đó là dấu hiệu nguồn có vấn đề, và không cỗ máy kiểm định nào thay được phán đoán này.
5. **Nạp.** `iceberg_loader` chạy MERGE theo SCD2: đóng bản ghi hiện hành bằng `valid_to = ngày hiệu lực`, `is_current = false`; chèn bản ghi mới `valid_from = ngày hiệu lực`, `valid_to = 9999-12-31`. Vì JOIN dùng `>= valid_from AND < valid_to`, hai khoảng ráp khít, không hở không đè.
6. **Đối chứng lại.** Chạy lại bộ điểm vàng. Kỳ vọng: thay đổi ở đúng địa bàn bị ảnh hưởng, **không thay đổi ở nơi khác**. Vế thứ hai mới là điều đáng kiểm — nó bắt được thay đổi ngoài ý muốn.

### 8.2 Điều các đội tiêu thụ không phải làm

Không chạy lại lịch sử. Không sửa job. Không đổi câu SQL. Dữ liệu cũ tra ra kết quả theo mô hình hành chính đương thời của nó, dữ liệu mới tra theo mô hình mới, cùng một câu SQL cho cả hai.

Đây là thứ mua được bằng việc chọn Sedona và SCD2. Nếu chọn "luôn theo mô hình mới nhất", mỗi nghị quyết kéo theo một đợt chạy lại toàn bộ lịch sử và một đợt giải thích với kiểm toán vì sao báo cáo tháng trước in ra khác lần trước.

### 8.3 Quy tắc vận hành

- **Ghim phiên bản.** Tag repo lưu trong config và trong cột `source_release` của từng dòng — tái lập được bảng từ đầu bất cứ lúc nào.
- **Không xoá phiên bản cũ.** Cấu hình dọn snapshot của Iceberg phải loại trừ bảng này, hoặc đặt thời gian giữ thật dài. Bảng chỉ vài nghìn dòng mỗi phiên bản; tiếc dung lượng ở đây là tiết kiệm nhầm chỗ.
- **Có chủ sở hữu.** Quy trình cần một đội đứng tên. Không có ai sở hữu thì đến nghị quyết thứ hai là nó thành đồ bỏ.

---

## 9. Phần mở rộng đã biết trước

Những việc **không** làm trong phạm vi này, ghi lại để khi cần thì đã có hướng.

### 9.1 Bảng ánh xạ xã cũ sang xã mới

SCD2 giữ lịch sử **đúng**, nhưng không làm cho lịch sử **cộng được**. Một báo cáo gom nhóm theo `ward_code` trải qua mốc sáp nhập sẽ thấy địa bàn cũ đột ngột về 0 và địa bàn mới xuất hiện từ hư không — số liệu không sai, nhưng không so sánh được theo thời gian.

Muốn so sánh xuyên mốc cần một bảng ánh xạ xã cũ → xã mới. Đây là bài toán riêng và có phần không giải được sạch: một xã cũ bị xẻ vào hai xã mới thì không có tỉ lệ quy đổi nào đúng tuyệt đối. Repo có sẵn `classify_changes.py` và `diff_postgres_units.py`, dùng làm điểm khởi đầu được.

### 9.2 Hút về xã gần nhất

Tham số `snap_tolerance_m` đã có trong giao diện, mặc định `0`. Bật lên khi số liệu ở mục 6.3 cho thấy cần, với bán kính chọn theo số liệu chứ không theo phỏng đoán.

### 9.3 Vá ranh giới giai đoạn 01/07/2025 → 29/04/2026

Khác biệt duy nhất so với hiện hành là việc lập TP. Đồng Nai. Khung SCD2 cho phép chèn thêm phiên bản này bất cứ lúc nào mà không phải làm lại gì.

### 9.4 Chuyển sang bảng tra cứu lưới H3

Nếu quy mô lên hàng tỷ dòng và ghép không gian thành nút thắt. Vì bảng chiều đã lưu geometry ở dạng WKB chuẩn, việc tiền tính lưới H3 là một job đọc thêm, không phải làm lại dữ liệu.

### 9.5 Cột vùng miền (`region_name`)

Thiết kế ban đầu có cột này, đã bỏ sau khi kiểm tra dữ liệu thật. Bảng `administrative_regions` vẫn tồn tại trong bộ dữ liệu (8 vùng: Đông Bắc Bộ, Tây Bắc Bộ, Đồng bằng sông Hồng...), nhưng **sau tái cấu trúc 2025 không còn trường nào ở cấp tỉnh trỏ tới nó** — cả lược đồ PostgreSQL lẫn `full_json_generated_data_vn_units.json` đều không có khoá ngoại về vùng. Nghĩa là không suy ra được vùng miền của một tỉnh từ bộ dữ liệu này.

Nếu về sau cần cột này, cách duy nhất là tự duy trì một bảng ánh xạ 34 dòng tỉnh → vùng. Rẻ về công sức nhưng là dữ liệu tự chế, phải có người chịu trách nhiệm cập nhật khi có tỉnh mới — nên chỉ làm khi có nhu cầu thật.

### 9.6 Tên đường và số nhà

Cần nguồn khác: OpenStreetMap hoặc dịch vụ thương mại. Bảng chiều ở đây vẫn giữ vai trò chuẩn hoá phần hành chính theo mô hình hai cấp — thứ mà các nguồn quốc tế thường trả về sai hoặc còn dùng dữ liệu quận/huyện cũ.

---

## 10. Tóm tắt các quyết định

| # | Quyết định | Lý do |
|---|---|---|
| 1 | Chỉ dùng dữ liệu trong repo, không gọi API ngoài | Yêu cầu của người dùng; đổi lại không có tên đường và số nhà |
| 2 | Ghép không gian bằng Sedona thay vì UDF vô hướng | Dữ liệu cập nhật thường xuyên; tách rời cập nhật dữ liệu khỏi triển khai code; ngữ nghĩa theo thời điểm tự nhiên hơn |
| 3 | SCD2 thay vì luôn lấy bản mới nhất | Tái lập được báo cáo lịch sử; yêu cầu kiểm toán ngân hàng |
| 4 | Dữ liệu trước mốc ranh giới sớm nhất vẫn tra, kèm cờ `geo_ref_is_exact = false` | Nguồn không có ranh giới lịch sử; nói rõ tốt hơn im lặng |
| 5 | Giữ nguyên độ phân giải polygon, không rút gọn | Geometry nằm trong bảng chứ không broadcast nên không tốn gì; cho độ chính xác tuyệt đối ở vùng giáp ranh |
| 6 | Đọc GeoJSON zip thay vì dump PostGIS | Không phải dựng Postgres trung chuyển cho một việc chạy vài lần một năm |
| 7 | Luôn `LEFT JOIN`, trả về `addr_match_status` | Không được làm mất bản ghi nghiệp vụ; lỗi phải nhìn thấy được |
| 8 | Chưa xây cơ chế hút về xã gần nhất | Chờ số liệu đo được rồi mới quyết định bán kính |
| 9 | Chưa xây bảng ánh xạ xã cũ sang xã mới | Bài toán riêng, có phần không giải được sạch; ghi nhận trước |
