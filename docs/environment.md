# Môi trường cụm — trạng thái xác minh

**Trạng thái: CHƯA XÁC MINH.** Task 8, 9, 10 của kế hoạch bị chặn cho tới khi
mục "Kết quả chạy thử" bên dưới có nội dung thật.

## Cần đội hạ tầng cung cấp

| Thông tin | Cách lấy | Giá trị |
|---|---|---|
| Phiên bản Spark | `spark-submit --version` | *chưa có* |
| Phiên bản Scala | dòng `Using Scala version ...` của lệnh trên | *chưa có* |
| Phiên bản Python của venv | `$PYSPARK_VENV_PATH/bin/python --version` | *chưa có* |
| Phiên bản Iceberg runtime | `SHOW TBLPROPERTIES` trên một bảng Iceberg sẵn có | *chưa có* |

## Toạ độ gói Sedona đã chọn

Điền sau khi tra bảng tương thích tại https://sedona.apache.org/latest/setup/maven-coordinates/

```
org.apache.sedona:sedona-spark-shaded-<spark_major_minor>_<scala>:<sedona_version>
org.datasyslab:geotools-wrapper:<geotools_version>
```

## Kết quả chạy thử

```bash
spark-submit \
  --packages <điền toạ độ gói ở trên> \
  scripts/sedona_smoke_test.py
```

- Ngày chạy: *chưa chạy*
- Kết quả: *chưa có*
- Dòng `--packages` nguyên văn đã chạy được: *chưa có*

**Nếu chạy thất bại và không cài được Sedona:** dừng kế hoạch, quay lại bước
thiết kế. Phương án dự phòng là UDF vô hướng (spec mục 3) và nó đòi kiến trúc
khác — không được chắp vá.

## Mốc hiệu năng

Ghi lại khi tích hợp vào job thật đầu tiên (kế hoạch, ghi chú về spec mục 7.4).

| Số điểm | Thời gian | `spark.sedona.join.gridtype` | Số executor | Ngày đo |
|---|---|---|---|---|
| *chưa đo* | | | | |

## Bộ điểm vàng

Đường dẫn tới file toạ độ chi nhánh/ATM nếu để ngoài git: *chưa có*
