from datetime import date, datetime, timezone
from pathlib import Path

from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon, box

from vn_geo_build.attribute_reader import read_units_json
from vn_geo_build.dim_builder import BuildReport, DimRow, build_dim
from vn_geo_build.geojson_reader import read_geojson_zip
from vn_geo_build.validator import validate

FIX = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 9, 3, tzinfo=timezone.utc)


def _build():
    features = list(read_geojson_zip(FIX / "mini_geojson.zip"))
    provinces, wards = read_units_json(FIX / "mini_units.json")
    rows, report, pgeoms = build_dim(
        features, provinces, wards,
        valid_from=date(2026, 4, 30), valid_to=date(9999, 12, 31),
        decree_ref="30/2026/QH16", source_release="v5.0.0", ingested_at=NOW,
    )
    return rows, report, pgeoms, set(wards), set(provinces)


def _validate(rows, report, pgeoms, wcodes, pcodes):
    return validate(rows, report, pgeoms,
                    expected_ward_codes=wcodes, expected_province_codes=pcodes)


def _rules(issues) -> set[str]:
    return {i.rule for i in issues}


def test_du_lieu_that_khong_vi_pham_luat_toan_ven():
    rows, report, pgeoms, wcodes, pcodes = _build()
    issues = _validate(rows, report, pgeoms, wcodes, pcodes)
    # Fixture chỉ có 3 xã nên hai luật quy mô toàn quốc chắc chắn vi phạm.
    # canh_bao_* là cảnh báo, không chặn xuất bản, nên cũng được phép xuất hiện.
    assert _rules(issues) <= {"tong_dien_tich", "khe_ho", "canh_bao_hinh_hoc_da_sua"}


def test_bat_duoc_xa_thieu_hinh_hoc():
    rows, report, pgeoms, wcodes, pcodes = _build()
    issues = _validate(rows, report, pgeoms, wcodes | {"99999"}, pcodes)
    assert "toan_ven_tham_chieu" in _rules(issues)


def test_bat_duoc_xa_thua_so_voi_thuoc_tinh():
    rows, report, pgeoms, wcodes, pcodes = _build()
    issues = _validate(rows, report, pgeoms, wcodes - {rows[0].ward_code}, pcodes)
    assert "toan_ven_tham_chieu" in _rules(issues)


def test_bat_duoc_hinh_hoc_khong_hop_le():
    rows, report, pgeoms, wcodes, pcodes = _build()
    bowtie = MultiPolygon([Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])])
    broken = [DimRow(**{**rows[0].__dict__, "geom": wkb.dumps(bowtie)})]
    issues = _validate(broken + rows[1:], report, pgeoms, wcodes, pcodes)
    assert "hinh_hoc_hop_le" in _rules(issues)


def test_bat_duoc_polygon_rong():
    rows, report, pgeoms, wcodes, pcodes = _build()
    empty = [DimRow(**{**rows[0].__dict__, "geom": wkb.dumps(MultiPolygon())})]
    issues = _validate(empty + rows[1:], report, pgeoms, wcodes, pcodes)
    assert "polygon_rong" in _rules(issues)


def test_bat_duoc_chong_lan():
    rows, report, pgeoms, wcodes, pcodes = _build()
    square = MultiPolygon([box(0, 0, 1, 1)])
    a = DimRow(**{**rows[0].__dict__, "geom": wkb.dumps(square)})
    b = DimRow(**{**rows[1].__dict__, "geom": wkb.dumps(square),
                  "province_code": a.province_code})
    issues = _validate([a, b], report, pgeoms, wcodes, pcodes)
    assert "chong_lan" in _rules(issues)


def test_bat_duoc_hinh_hoc_ghep_nham_thuoc_tinh():
    """Thư mục tỉnh trong zip GeoJSON lệch ProvinceCode trong JSON."""
    rows, report, pgeoms, wcodes, pcodes = _build()
    lech = BuildReport(**{**report.__dict__,
                          "province_code_mismatches": (("00004", "79", "01"),)})
    issues = _validate(rows, lech, pgeoms, wcodes, pcodes)
    assert "nhat_quan_cheo" in _rules(issues)


def test_xa_ngoai_polygon_cap_tinh_chi_la_canh_bao_khong_chan():
    """Lớp cấp tỉnh bỏ sót đảo, nên nằm ngoài nó không được phép chặn."""
    rows, report, pgeoms, wcodes, pcodes = _build()
    xa_dao = MultiPolygon([box(0, 0, 1, 1)])  # ngoài lãnh thổ VN
    moved = [DimRow(**{**rows[0].__dict__, "geom": wkb.dumps(xa_dao)})]
    issues = _validate(moved + rows[1:], report, pgeoms, wcodes, pcodes)
    chan = {i.rule for i in issues if not i.rule.startswith("canh_bao_")}
    assert "canh_bao_ngoai_lop_tinh" in _rules(issues)
    assert "nhat_quan_cheo" not in chan


def test_bat_duoc_scd2_chong_lan_khoang_thoi_gian():
    rows, report, pgeoms, wcodes, pcodes = _build()
    dup = DimRow(**{**rows[0].__dict__, "valid_from": date(2026, 1, 1)})
    issues = _validate([rows[0], dup] + rows[1:], report, pgeoms, wcodes, pcodes)
    assert "toan_ven_scd2" in _rules(issues)


def test_bat_duoc_tong_dien_tich_lech_qua_nguong():
    rows, report, pgeoms, wcodes, pcodes = _build()
    huge = BuildReport(**{**report.__dict__})
    inflated = [DimRow(**{**r.__dict__, "area_km2": 1_000_000.0}) for r in rows]
    issues = _validate(inflated, huge, pgeoms, wcodes, pcodes)
    assert "tong_dien_tich" in _rules(issues)


def test_ham_do_luon_tra_ve_so_do_ke_ca_khi_trong_nguong():
    """Số đo phải có mặt kể cả lúc không vi phạm — đó là điểm của nó."""
    from vn_geo_build.validator import measure

    rows, *_ = _build()
    m = measure(rows)
    assert set(m) == {"overlap_ratio", "max_gap_ratio", "max_gap_province",
                      "total_area_km2"}
    assert m["overlap_ratio"] >= 0
    assert m["total_area_km2"] > 0
