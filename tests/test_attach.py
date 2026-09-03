"""Kiểm thử hàm gắn địa chỉ. Cần Spark local kèm Sedona."""
from datetime import date

import pytest
from shapely import wkb
from shapely.geometry import box

from vn_geo.attach import attach_vn_address

pytestmark = pytest.mark.spark

# Hai ô vuông kề nhau, không chồng lấn: dùng làm hai "xã" giả lập.
WEST = box(105.0, 21.0, 106.0, 22.0)
EAST = box(106.0, 21.0, 107.0, 22.0)

DIM_COLS = ["ward_code", "ward_full_name", "province_full_name", "postal_code",
            "geom", "valid_from", "valid_to", "is_current"]


@pytest.fixture
def dim(spark):
    rows = [
        ("00001", "Phường Tây", "Thành phố Thử", "11111",
         bytearray(wkb.dumps(WEST)), date(2026, 4, 30), date(9999, 12, 31), True),
        ("00002", "Phường Đông", "Thành phố Thử", "22222",
         bytearray(wkb.dumps(EAST)), date(2026, 4, 30), date(9999, 12, 31), True),
    ]
    spark.createDataFrame(rows, DIM_COLS).createOrReplaceTempView("dim_test")
    return "dim_test"


def _points(spark, rows):
    return spark.createDataFrame(rows, ["id", "lat", "lon", "txn_date"])


def test_diem_trong_polygon_ra_dung_xa(spark, dim):
    df = _points(spark, [(1, 21.5, 105.5, date(2026, 6, 1))])
    out = attach_vn_address(df, lat_col="lat", lon_col="lon",
                            as_of_col="txn_date", dim_table=dim).collect()
    assert out[0]["addr_ward_full_name"] == "Phường Tây"
    assert out[0]["addr_match_status"] == "MATCHED"
    assert out[0]["addr_geo_ref_is_exact"] is True


def test_so_dong_vao_bang_so_dong_ra(spark, dim):
    rows = [(i, 21.5, 105.5, date(2026, 6, 1)) for i in range(50)]
    rows += [(99, 0.0, 0.0, date(2026, 6, 1))]          # Null Island
    rows += [(98, 51.5, 0.1, date(2026, 6, 1))]         # ngoài VN
    rows += [(97, None, None, date(2026, 6, 1))]        # rỗng
    df = _points(spark, rows)
    out = attach_vn_address(df, lat_col="lat", lon_col="lon",
                            as_of_col="txn_date", dim_table=dim)
    assert out.count() == df.count()


def test_toa_do_rong_va_null_island_ra_INVALID_INPUT(spark, dim):
    df = _points(spark, [(1, None, None, date(2026, 6, 1)),
                         (2, 0.0, 0.0, date(2026, 6, 1))])
    out = {r["id"]: r["addr_match_status"] for r in
           attach_vn_address(df, lat_col="lat", lon_col="lon",
                             as_of_col="txn_date", dim_table=dim).collect()}
    assert out == {1: "INVALID_INPUT", 2: "INVALID_INPUT"}


def test_lat_lon_dao_nguoc_ra_INVALID_INPUT(spark, dim):
    """lat=105.5, lon=21.5 là cặp bị hoán vị; lat vượt 90 nên phát hiện được."""
    df = _points(spark, [(1, 105.5, 21.5, date(2026, 6, 1))])
    out = attach_vn_address(df, lat_col="lat", lon_col="lon",
                            as_of_col="txn_date", dim_table=dim).collect()
    assert out[0]["addr_match_status"] == "INVALID_INPUT"


def test_diem_ngoai_moi_polygon_ra_OUT_OF_BOUNDS(spark, dim):
    df = _points(spark, [(1, 21.5, 110.0, date(2026, 6, 1))])
    out = attach_vn_address(df, lat_col="lat", lon_col="lon",
                            as_of_col="txn_date", dim_table=dim).collect()
    assert out[0]["addr_match_status"] == "OUT_OF_BOUNDS"
    assert out[0]["addr_ward_full_name"] is None


def test_du_lieu_truoc_moc_ranh_gioi_co_co_geo_ref_is_exact_false(spark, dim):
    df = _points(spark, [(1, 21.5, 105.5, date(2024, 1, 1))])
    out = attach_vn_address(df, lat_col="lat", lon_col="lon",
                            as_of_col="txn_date", dim_table=dim).collect()
    assert out[0]["addr_ward_full_name"] == "Phường Tây", "van phai tra ket qua"
    assert out[0]["addr_geo_ref_is_exact"] is False


def test_bo_trong_as_of_col_thi_dung_ban_hien_hanh(spark, dim):
    df = spark.createDataFrame([(1, 21.5, 105.5)], ["id", "lat", "lon"])
    out = attach_vn_address(df, lat_col="lat", lon_col="lon",
                            dim_table=dim).collect()
    assert out[0]["addr_ward_full_name"] == "Phường Tây"


def test_diem_khop_hai_polygon_khong_nhan_doi_dong(spark):
    """Hai polygon chồng lấn: một dòng vào vẫn phải là một dòng ra."""
    overlap_a, overlap_b = box(105.0, 21.0, 106.0, 22.0), box(105.5, 21.0, 107.0, 22.0)
    rows = [
        ("00001", "Phường A", "Thành phố Thử", "11111",
         bytearray(wkb.dumps(overlap_a)), date(2026, 4, 30), date(9999, 12, 31), True),
        ("00002", "Phường B", "Thành phố Thử", "22222",
         bytearray(wkb.dumps(overlap_b)), date(2026, 4, 30), date(9999, 12, 31), True),
    ]
    spark.createDataFrame(rows, DIM_COLS).createOrReplaceTempView("dim_overlap")

    df = spark.createDataFrame([(1, 21.5, 105.7, date(2026, 6, 1))],
                               ["id", "lat", "lon", "txn_date"])
    out = attach_vn_address(df, lat_col="lat", lon_col="lon",
                            as_of_col="txn_date", dim_table="dim_overlap").collect()
    assert len(out) == 1
    assert out[0]["addr_match_status"] == "AMBIGUOUS"
    assert out[0]["addr_ward_code"] == "00001", "phai chon tat dinh, ma nho nhat"
