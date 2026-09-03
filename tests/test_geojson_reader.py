from pathlib import Path

from shapely.geometry.base import BaseGeometry
from vn_geo_build.geojson_reader import read_geojson_zip

FIXTURE = Path(__file__).parent / "fixtures" / "mini_geojson.zip"


def _by_code(level: str) -> dict:
    return {f.code: f for f in read_geojson_zip(FIXTURE) if f.level == level}


def test_doc_du_ca_hai_cap():
    provinces = _by_code("province")
    wards = _by_code("ward")
    assert set(provinces) == {"01", "79"}
    assert len(wards) == 3


def test_thuoc_tinh_cap_xa_dung():
    w = _by_code("ward")["00004"]
    assert w.name == "Ba Đình"
    assert w.full_name == "Phường Ba Đình"
    assert w.full_name_en == "Ba Dinh Ward"
    assert w.code_name == "ba_dinh"
    assert w.postal_code == "11120"
    assert w.province_code == "01"
    assert w.area_km2 > 0


def test_hinh_hoc_la_shapely_hop_le():
    w = _by_code("ward")["00004"]
    assert isinstance(w.geometry, BaseGeometry)
    assert w.geometry.geom_type == "MultiPolygon"
    assert not w.geometry.is_empty


def test_ma_tinh_cat_o_gach_duoi_dau_tien():
    """Thư mục dạng 19_tinh_quang_tri phải cho mã 19, không phải 'tri'."""
    from vn_geo_build.geojson_reader import province_code_from_dir

    assert province_code_from_dir("01_ha_noi") == "01"
    assert province_code_from_dir("19_tinh_quang_tri") == "19"
    assert province_code_from_dir("13_hai_phong") == "13"
