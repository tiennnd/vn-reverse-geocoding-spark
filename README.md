# Tra cứu địa chỉ hành chính Việt Nam từ toạ độ GPS

Thành phần dùng chung cho các job Spark SQL trên datalake: từ `(lat, lon)` trả về phường/xã, tỉnh/thành, mã đơn vị hành chính và mã bưu chính, theo mô hình hành chính hai cấp tại thời điểm dữ liệu phát sinh.

Nguồn dữ liệu: [thanglequoc/vietnamese-provinces-database](https://github.com/thanglequoc/vietnamese-provinces-database).

## Tài liệu

- [Thiết kế](docs/superpowers/specs/2026-09-03-vn-reverse-geocoding-spark-design.md) — phạm vi, kiến trúc, lược đồ bảng, kiểm thử, vận hành.
