# Runbook: cập nhật khi có nghị quyết hành chính mới

Nhịp thực tế theo lịch sử phát hành của repo nguồn: **3–4 lần một năm**.

**Chủ sở hữu quy trình:** *chưa phân công — phải điền trước khi đưa vào vận hành.*

## 1. Phát hiện (kiểm tra hàng quý)

Mở https://github.com/thanglequoc/vietnamese-provinces-database/releases
và so tag mới nhất với `RELEASE_TAG` trong `src/vn_geo_build/config.py`.

Không dựng sensor tự động: cụm thường không ra được internet, và mỗi quý
một lần thì một dòng trong lịch vận hành rẻ hơn một dịch vụ phải nuôi.

## 2. Xác định ngày hiệu lực

Đọc ghi chú phát hành, lấy số hiệu nghị quyết và **NGÀY CÓ HIỆU LỰC**.

Đây **không** phải ngày repo phát hành. Hai mốc lệch nhau hàng tuần đến hàng
tháng — bản `v3.0.2` phát hành 20/07/2025 cho một nghị quyết có hiệu lực từ
01/07/2025. Lấy nhầm mốc thì mọi giao dịch trong khoảng lệch bị gắn sai phiên
bản hành chính, và không ai phát hiện cho tới lúc đối soát.

## 3. Dựng và kiểm định

```bash
# a. Cập nhật RELEASE_TAG, VALID_FROM, DECREE_REF trong src/vn_geo_build/config.py

# b. Lấy checksum mới của hai file nguồn tại tag mới
TAG=<tag_moi>
BASE=https://raw.githubusercontent.com/thanglequoc/vietnamese-provinces-database/$TAG
curl -sL $BASE/json/vn_provinces_wards_geojson.zip        | shasum -a 256
curl -sL $BASE/json/full_json_generated_data_vn_units.json | shasum -a 256
# -> dán vào SOURCE_CHECKSUMS trong config.py

# c. Dựng
python -m vn_geo_build.cli --out build/dim_$TAG.parquet

# d. So với phiên bản đang chạy
python -c "
from vn_geo_build.diff_report import diff, format_report
print(format_report(diff('build/dim_v5.0.0.parquet', 'build/dim_$TAG.parquet')))
"
```

Kiểm định thất bại thì **không nới ngưỡng để cho qua**. Ngưỡng nới là ngưỡng
vô dụng. Đọc từng lỗi và đối chiếu spec mục 7.1.

## 4. Người duyệt

Gửi báo cáo khác biệt cho người duyệt. Đây là cửa người có chủ ý.

Cảnh báo: nếu một nghị quyết lẽ ra chỉ đổi một tỉnh mà báo cáo cho thấy hàng
trăm xã đổi ranh giới, đó là dấu hiệu nguồn có vấn đề. Dừng lại và điều tra —
không cỗ máy kiểm định nào thay được phán đoán này.

## 5. Nạp

```bash
spark-submit --packages <xem docs/environment.md> \
  src/vn_geo_load/iceberg_loader.py \
  --parquet-path <duong dan hdfs> \
  --dim-table dim.dim_vn_ward_gis \
  --valid-from <ngay hieu luc> \
  --decree-ref <so nghi quyet> \
  --source-release <tag>
```

## 6. Đối chứng lại

`verify_accuracy.py` import `vn_geo`. Nếu venv trên cụm chưa cài package này
thì đóng gói và gửi kèm bằng `--py-files`, thiếu là `ModuleNotFoundError`:

```bash
(cd src && zip -qr ../build/vn_geo.zip vn_geo -x '*__pycache__*')

spark-submit --packages <...> --py-files build/vn_geo.zip \
  scripts/verify_accuracy.py \
  --dim-table dim.dim_vn_ward_gis \
  --only-representative
```

`--only-representative` chỉ chạy bài điểm đại diện (phải đạt 100%). Bỏ cờ này
khi đã có bộ điểm vàng `golden/golden_points.csv` — thiếu file mà không có cờ
thì script hỏng.

Kỳ vọng: thay đổi ở đúng địa bàn bị ảnh hưởng, **không thay đổi ở nơi khác**.
Vế thứ hai mới là điều đáng kiểm — nó bắt được thay đổi ngoài ý muốn.

## Không được làm

- Không xoá phiên bản cũ khỏi bảng.
- Không đặt lịch dọn snapshot Iceberg ngắn cho bảng này.
- Không nới ngưỡng kiểm định để cho qua.
- Không lấy ngày phát hành repo làm `valid_from`.
