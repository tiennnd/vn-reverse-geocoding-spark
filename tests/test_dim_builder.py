from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon

from vn_geo_build.attribute_reader import read_units_json
from vn_geo_build.dim_builder import build_dim
from vn_geo_build.geojson_reader import GeoFeature, read_geojson_zip

FIX = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 9, 3, tzinfo=timezone.utc)


def _build():
    features = list(read_geojson_zip(FIX / "mini_geojson.zip"))
    provinces, wards = read_units_json(FIX / "mini_units.json")
    return build_dim(
        features, provinces, wards,
        valid_from=date(2026, 4, 30), valid_to=date(9999, 12, 31),
        decree_ref="30/2026/QH16", source_release="v5.0.0", ingested_at=NOW,
    )


def test_moi_xa_mot_dong():
    rows, report, _ = _build()
    assert len(rows) == 3
    assert report.n_wards == 3
    assert report.n_provinces == 2


def test_ghep_thuoc_tinh_tinh_vao_dong_xa():
    rows, _, _ = _build()
    r = next(r for r in rows if r.ward_code == "00004")
    assert r.province_full_name == "Thành phố Hà Nội"
    assert r.ward_type == "Phường"
    assert r.postal_code == "11120"


def test_geom_va_bbox_la_wkb_giai_ma_duoc():
    rows, _, _ = _build()
    r = rows[0]
    g = wkb.loads(r.geom)
    b = wkb.loads(r.bbox)
    assert g.geom_type == "MultiPolygon"
    assert b.geom_type == "Polygon"
    assert b.contains(g), "bbox phai bao trum geom"


def test_cot_scd2_duoc_gan_dung():
    rows, _, _ = _build()
    r = rows[0]
    assert r.valid_from == date(2026, 4, 30)
    assert r.valid_to == date(9999, 12, 31)
    assert r.is_current is True
    assert r.decree_ref == "30/2026/QH16"
    assert r.source_release == "v5.0.0"


def test_hinh_hoc_hong_duoc_sua_va_dem_lai():
    """Polygon hình số 8 tự cắt: phải được sửa, và đếm vào báo cáo."""
    bowtie = Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])
    assert not bowtie.is_valid
    feat = GeoFeature(
        code="00004", level="ward", province_code="01", name="Ba Đình",
        name_en="Ba Dinh", full_name="Phường Ba Đình", full_name_en="Ba Dinh Ward",
        code_name="ba_dinh", postal_code="11120", postal_code_prefix="",
        area_km2=2.97, gis_server_id="x", geometry=MultiPolygon([bowtie]),
    )
    provinces, wards = read_units_json(FIX / "mini_units.json")
    prov_feat = next(
        f for f in read_geojson_zip(FIX / "mini_geojson.zip")
        if f.level == "province" and f.code == "01"
    )
    rows, report, _ = build_dim(
        [feat, prov_feat], provinces, wards,
        valid_from=date(2026, 4, 30), valid_to=date(9999, 12, 31),
        decree_ref="30/2026/QH16", source_release="v5.0.0", ingested_at=NOW,
    )
    assert report.n_geometries_fixed == 1
    assert wkb.loads(rows[0].geom).is_valid


def test_xa_thieu_thuoc_tinh_thi_bao_loi_ngay():
    prov_feat = next(
        f for f in read_geojson_zip(FIX / "mini_geojson.zip")
        if f.level == "province" and f.code == "01"
    )
    ghost = GeoFeature(
        code="99999", level="ward", province_code="01", name="Ma", name_en="",
        full_name="", full_name_en="", code_name="", postal_code="",
        postal_code_prefix="", area_km2=1.0, gis_server_id="",
        geometry=MultiPolygon([Polygon([(0, 0), (0, 1), (1, 1), (0, 0)])]),
    )
    provinces, wards = read_units_json(FIX / "mini_units.json")
    with pytest.raises(KeyError, match="99999"):
        build_dim(
            [ghost, prov_feat], provinces, wards,
            valid_from=date(2026, 4, 30), valid_to=date(9999, 12, 31),
            decree_ref="30/2026/QH16", source_release="v5.0.0", ingested_at=NOW,
        )
