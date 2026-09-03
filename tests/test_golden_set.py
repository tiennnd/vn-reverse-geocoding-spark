"""Kiểm tra hai điểm sát hai bên một đường ranh giới ra hai xã khác nhau.

Đây là bài đo trực tiếp lợi thế của phương án đã chọn: giữ nguyên độ phân
giải polygon nên phân biệt được ở cỡ mét, khác với bảng tra cứu lưới H3.
"""
from datetime import date

import pytest
from shapely import wkb
from shapely.geometry import box

from vn_geo.attach import attach_vn_address

pytestmark = pytest.mark.spark

WEST = box(105.0, 21.0, 106.0, 22.0)
EAST = box(106.0, 21.0, 107.0, 22.0)
DELTA_10M = 0.0001  # ~11 m theo kinh độ ở vĩ độ 21


def test_hai_ben_ranh_gioi_ra_hai_xa_khac_nhau(spark):
    rows = [
        ("00001", "Phường Tây", bytearray(wkb.dumps(WEST)),
         date(2026, 4, 30), date(9999, 12, 31), True),
        ("00002", "Phường Đông", bytearray(wkb.dumps(EAST)),
         date(2026, 4, 30), date(9999, 12, 31), True),
    ]
    spark.createDataFrame(
        rows, ["ward_code", "ward_full_name", "geom",
               "valid_from", "valid_to", "is_current"]
    ).createOrReplaceTempView("dim_bien")

    pts = spark.createDataFrame(
        [("tay", 21.5, 106.0 - DELTA_10M), ("dong", 21.5, 106.0 + DELTA_10M)],
        ["label", "lat", "lon"],
    )
    out = {r["label"]: r["addr_ward_code"] for r in
           attach_vn_address(pts, lat_col="lat", lon_col="lon",
                             fields=("ward_code", "ward_full_name"),
                             dim_table="dim_bien").collect()}

    assert out["tay"] == "00001"
    assert out["dong"] == "00002"
