# Môi trường — trạng thái xác minh

Có **hai** môi trường và chúng không giống nhau. Đừng lẫn.

---

## A. Máy phát triển — ✅ ĐÃ XÁC MINH 03/09/2026

Dùng để phát triển và kiểm thử Task 8–10. Không phải môi trường chạy thật.

| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| Java | OpenJDK **17.0.20.1** | `brew install openjdk@17`, keg-only |
| `JAVA_HOME` | `/opt/homebrew/opt/openjdk@17` | `conftest.py` tự đặt nếu biến chưa có |
| Spark / PySpark | **4.0.4** | Cài qua pip vào `.venv` |
| Scala | **2.13** | Từ Spark 4.0 không còn lựa chọn nào khác |
| Python | **3.13** | PySpark 4.0.4 khai báo hỗ trợ |
| Sedona | **1.9.1** | |
| Iceberg | **1.11.0** | |

Toạ độ gói đã chạy được, nguyên văn:

```
org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.9.1
org.datasyslab:geotools-wrapper:1.9.1-33.5
org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0
```

Kết quả bài thử `scripts/sedona_smoke_test.py`: **`SEDONA OK | spark 4.0.4`**

### Vì sao Spark 4.0 chứ không phải 4.1 hay 4.2

- **4.2 bị loại thẳng**: Sedona chưa có bản dựng nào cho Spark 4.2
  (`sedona-spark-shaded-4.2_2.13` trả về HTTP 404).
- **4.1**: Sedona 1.9.0 và 1.9.1 có hỗ trợ, nhưng chỉ hai bản.
- **4.0**: có bốn bản Sedona (1.8.0, 1.8.1, 1.9.0, 1.9.1). Nếu 1.9.1 giở
  chứng thì lùi được về 1.8.1 mà không đụng tới Spark. 4.1 không có đường lùi đó.

`geotools-wrapper` phải là **1.9.1-33.5** — số đầu bám sát phiên bản Sedona.
Lệch một nấc là lỗi lúc chạy, không phải lúc tải gói.

### Cạm bẫy đã gặp

`apache-sedona` **không khai báo `pandas` là phụ thuộc bắt buộc** nhưng lại
`import pandas` ngay lúc nạp module. Thiếu là `ModuleNotFoundError` khi
`import sedona.spark`. Đã thêm `pandas` vào nhóm phụ thuộc `spark` trong
`pyproject.toml`.

---

## B. Cụm ngân hàng — ⚠️ CHƯA XÁC MINH

**Việc máy phát triển chạy Spark 4.0 không có nghĩa cụm cũng vậy.** Cụm nhiều
khả năng còn ở Spark 3.x, và toàn bộ toạ độ gói ở mục A sẽ **không** dùng
được ở đó. Task 1 vẫn còn nguyên giá trị.

| Thông tin | Cách lấy | Giá trị |
|---|---|---|
| Phiên bản Spark | `spark-submit --version` | *chưa có* |
| Phiên bản Scala | dòng `Using Scala version ...` | *chưa có* |
| Python của venv | `$PYSPARK_VENV_PATH/bin/python --version` | *chưa có* |
| Iceberg runtime | `SHOW TBLPROPERTIES` trên bảng Iceberg sẵn có | *chưa có* |

Bảng tương thích Sedona: https://sedona.apache.org/latest/setup/maven-coordinates/

Đối chiếu nhanh cho Spark 3.5 / Scala 2.12, nếu cụm là bản đó:

```
org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.1
org.datasyslab:geotools-wrapper:1.9.1-33.5
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0
```

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
