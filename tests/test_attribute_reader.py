from pathlib import Path

from vn_geo_build.attribute_reader import read_units_json

FIXTURE = Path(__file__).parent / "fixtures" / "mini_units.json"


def test_doc_duoc_ca_tinh_va_xa():
    provinces, wards = read_units_json(FIXTURE)
    assert set(provinces) == {"01", "79"}
    assert len(wards) == 3


def test_thuoc_tinh_cap_tinh():
    provinces, _ = read_units_json(FIXTURE)
    p = provinces["01"]
    assert p.province_name == "Hà Nội"
    assert p.province_full_name == "Thành phố Hà Nội"
    assert p.province_type == "Thành phố"
    assert "10" in p.province_postal_code_prefix


def test_loai_don_vi_cap_xa_chi_co_o_nguon_json():
    _, wards = read_units_json(FIXTURE)
    w = wards["00004"]
    assert w.ward_type == "Phường"
    assert w.province_code == "01"
    assert w.ward_full_name == "Phường Ba Đình"


def test_moi_loai_don_vi_deu_nam_trong_ba_gia_tri_hop_le():
    _, wards = read_units_json(FIXTURE)
    assert {w.ward_type for w in wards.values()} <= {"Phường", "Xã", "Đặc khu"}
