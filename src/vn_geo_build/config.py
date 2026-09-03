"""Hằng số dùng chung cho nửa dựng dữ liệu."""
from datetime import date

REPO = "thanglequoc/vietnamese-provinces-database"
RELEASE_TAG = "v5.0.0"
RAW_BASE = f"https://raw.githubusercontent.com/{REPO}/{RELEASE_TAG}"

GEOJSON_ZIP_URL = f"{RAW_BASE}/json/vn_provinces_wards_geojson.zip"
UNITS_JSON_URL = f"{RAW_BASE}/json/full_json_generated_data_vn_units.json"

# Băm của đúng hai file trên tại tag RELEASE_TAG. Đổi tag thì phải đổi cả hai.
SOURCE_CHECKSUMS = {
    "vn_provinces_wards_geojson.zip":
        "5d92c003d7fd379dccd781e72603016126e5a8d16224f54acd16f4ff01705f7e",
    "full_json_generated_data_vn_units.json":
        "323af8709651389638d7f6eda98e49d368d7c25ee103294050c84054094fb76a",
}

# Phiên bản SCD2 đầu tiên. valid_from là ngày nghị quyết có hiệu lực,
# KHÔNG phải ngày repo phát hành. Xem spec mục 8.1 bước 2.
DECREE_REF = "30/2026/QH16"
VALID_FROM = date(2026, 4, 30)
FAR_FUTURE = date(9999, 12, 31)

# Ngưỡng kiểm định (spec mục 7.1)
TOTAL_AREA_KM2 = 331_000.0
AREA_TOLERANCE = 0.02
OVERLAP_TOLERANCE = 0.0001
GAP_TOLERANCE = 0.0001
