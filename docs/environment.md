# Môi trường — trạng thái xác minh

Có **hai** môi trường và chúng không giống nhau. Đừng lẫn.

---

## A. Máy phát triển — ✅ ĐÃ XÁC MINH 22/10/2026

Dùng để phát triển và kiểm thử Task 8–10. Không phải môi trường chạy thật.
Từ 22/10/2026 cấu hình **khớp Spark/Scala của cụm** (mục B), để không vô tình
dùng API chỉ có ở Spark 4.

| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| Java | OpenJDK **17.0.20.1** | `brew install openjdk@17`, keg-only |
| `JAVA_HOME` | `/opt/homebrew/opt/openjdk@17` | `conftest.py` tự đặt nếu biến chưa có |
| Spark / PySpark | **3.5.3** | Cài qua pip vào `.venv` |
| Scala | **2.12** | Khớp cụm (2.12.18) |
| Python | **3.11** | PySpark 3.5.3 chỉ hỗ trợ tới 3.11 — xem mục B |
| Sedona | **1.9.1** | |
| Iceberg | **1.11.0** | Cần Java 17 — xem mục B |

Dựng lại `.venv`:

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -e ".[spark,dev]"
```

Toạ độ gói đã chạy được, nguyên văn (cũng là mặc định trong `conftest.py`
và `scripts/local_end_to_end.py`):

```
org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.1
org.datasyslab:geotools-wrapper:1.9.1-33.5
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0
```

Kết quả bài thử `scripts/sedona_smoke_test.py`: **`SEDONA OK`**

`conftest.py` và `scripts/local_end_to_end.py` tự đặt `PYSPARK_PYTHON` bằng
python đang chạy. Thiếu bước này thì worker lấy `python3` trong PATH (hệ
thống là 3.13) và mọi test Spark hỏng với `PYTHON_VERSION_MISMATCH`. Khi tự
gọi `spark-submit` trên máy thì phải tự đặt biến này.

### Toàn trình đã chạy được trên máy (22/10/2026)

`pytest`: **43/43 đạt**.

`python scripts/local_end_to_end.py` — tạo bảng Iceberg thật, nạp 3321 xã, rồi
kiểm chứng qua đúng đường Sedona mà job thật sẽ đi:

- Nạp: 3321 dòng vào `local.dim.dim_vn_ward_gis`
- **Bài điểm đại diện: 3321/3321 = 100,0000%** — bài duy nhất phải đạt tuyệt đối
- Bốn trạng thái đều đúng: `MATCHED`, `OUT_OF_BOUNDS` (Biển Đông), `INVALID_INPUT`
  (toạ độ đảo ngược và Null Island), và `geo_ref_is_exact = false` cho giao dịch
  năm 2024
- Đảo Trường Sa Lớn tra ra `Đặc khu Trường Sa, Tỉnh Khánh Hoà`

Máy chạy đúng phiên bản Spark/Scala của cụm nhưng không thay được bài thử
trên cụm: JVM, venv và catalog của cụm vẫn chưa được xác nhận.

### Giới hạn của catalog cục bộ

Catalog Iceberg kiểu `hadoop` **không hỗ trợ view** (`UnsupportedOperationException:
Replacing a view is not supported`). Đây là giới hạn của môi trường máy, không
phải của thiết kế — catalog Hive hoặc REST trên cụm tạo view bình thường.
`scripts/local_end_to_end.py` tự lùi về temp view khi gặp lỗi này.

### Lịch sử: Spark 4.0.4 (03/09 → 22/10/2026)

Trước khi biết phiên bản cụm, máy chạy Spark 4.0.4 / Scala 2.13 / Python 3.13
với các gói `*-4.0_2.13` cùng phiên bản Sedona và Iceberg như trên, cũng đạt
toàn bộ test và bài điểm đại diện 100%. Code chạy được trên cả hai dòng Spark
mà không phải sửa.

`geotools-wrapper` phải là **1.9.1-33.5** — số đầu bám sát phiên bản Sedona.
Lệch một nấc là lỗi lúc chạy, không phải lúc tải gói.

### Cạm bẫy đã gặp

`apache-sedona` **không khai báo `pandas` là phụ thuộc bắt buộc** nhưng lại
`import pandas` ngay lúc nạp module. Thiếu là `ModuleNotFoundError` khi
`import sedona.spark`. Đã thêm `pandas` vào nhóm phụ thuộc `spark` trong
`pyproject.toml`.

---

## B. Cụm ngân hàng — ⚠️ XÁC MINH MỘT PHẦN (26/10/2026)

Cụm chạy **Spark 3.5.3 / Scala 2.12.18 / Java 17.0.15 / Python 3.11**. Máy
phát triển (mục A) cùng major Java và cùng minor Python, chạy đạt toàn bộ test.
Bộ toạ độ gói dưới đây dùng được nguyên vẹn.

| Thông tin | Cách lấy | Giá trị |
|---|---|---|
| Phiên bản Spark | `spark-submit --version` | **3.5.3** |
| Phiên bản Scala | dòng `Using Scala version ...` | **2.12.18** |
| Phiên bản Java | dòng `Using Scala version ..., <JVM> <phiên bản>` | **17.0.15** — dùng nguyên bộ gói dưới |
| Python của venv | `$PYSPARK_VENV_PATH/bin/python --version` | **3.11** |
| Iceberg runtime | `SHOW TBLPROPERTIES` trên bảng Iceberg sẵn có | *chưa có* |

Bảng tương thích Sedona: https://sedona.apache.org/latest/setup/maven-coordinates/

### Toạ độ gói cho Spark 3.5 / Scala 2.12

Cả ba đều có trên Maven Central và đã chạy được trên máy (mục A):

```
org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.1
org.datasyslab:geotools-wrapper:1.9.1-33.5
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0
```

### Ràng buộc Java — phải hỏi trước khi chọn gói

Đo phiên bản bytecode trong jar (major 55 = Java 11, 61 = Java 17):

| Gói | Java tối thiểu |
|---|---|
| `sedona-spark-shaded-3.5_2.12:1.9.1` | 11 |
| `geotools-wrapper:1.9.1-33.5` | 11 |
| `iceberg-spark-runtime-3.5_2.12:1.11.0` | **17** |
| `iceberg-spark-runtime-3.5_2.12:1.10.1` trở xuống | 11 |

- **Java 17:** dùng nguyên bộ trên. ← **cụm thuộc trường hợp này**
- **Java 11:** Iceberg phải ≤ 1.10.1.
- **Java 8:** Spark 3.5 vẫn chạy, nhưng **Sedona 1.9.1 thì không**. Phải tìm
  bản Sedona cũ hơn còn biên dịch cho Java 8, hoặc xem lại hướng thiết kế.

Iceberg nên lấy **đúng bản cụm đang dùng** cho các bảng sẵn có, không tự
mang bản riêng vào.

### Ràng buộc Python

PySpark 3.5.3 chỉ khai báo hỗ trợ Python 3.8–3.11, và còn
`from distutils.version import LooseVersion` — module đã bị gỡ khỏi Python 3.12.
Venv trên cụm phải là **≤ 3.11**.

Kết quả chạy `scripts/sedona_smoke_test.py` trên cụm: *chưa chạy*

Đổi phiên bản thì đặt biến môi trường `VN_GEO_SPARK_PACKAGES`, không sửa code.

---

## Mốc hiệu năng

Ghi lại khi tích hợp vào job thật đầu tiên (spec mục 7.4).

| Số điểm | Thời gian | `spark.sedona.join.gridtype` | Số executor | Ngày đo |
|---|---|---|---|---|
| *chưa đo* | | | | |

## Bộ điểm vàng

Đường dẫn tới file toạ độ chi nhánh/ATM nếu để ngoài git: *chưa có*
