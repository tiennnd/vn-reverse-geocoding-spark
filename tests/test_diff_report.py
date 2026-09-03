from datetime import date, datetime, timezone

import pyarrow.parquet as pq
import pytest
from shapely import wkb
from shapely.geometry import box

from vn_geo_build.cli import rows_to_table
from vn_geo_build.dim_builder import DimRow
from vn_geo_build.diff_report import diff


def _row(ward_code: str, ward_full_name: str, geom_box) -> DimRow:
    g = wkb.dumps(geom_box)
    return DimRow(
        ward_code=ward_code, ward_name=ward_full_name.split()[-1],
        ward_full_name=ward_full_name, ward_name_en="", ward_full_name_en="",
        ward_code_name="", ward_type="Phường", postal_code="",
        province_code="01", province_name="Hà Nội",
        province_full_name="Thành phố Hà Nội", province_name_en="",
        province_postal_code_prefix="", bbox=g, geom=g, area_km2=1.0,
        valid_from=date(2026, 4, 30), valid_to=date(9999, 12, 31),
        is_current=True, decree_ref="30/2026/QH16", source_release="v5.0.0",
        ingested_at=datetime(2026, 9, 3, tzinfo=timezone.utc),
    )


@pytest.fixture
def paths(tmp_path):
    old = [_row("00001", "Phường A", box(0, 0, 1, 1)),
           _row("00002", "Phường B", box(1, 0, 2, 1)),
           _row("00003", "Phường C", box(2, 0, 3, 1))]
    new = [_row("00001", "Phường A", box(0, 0, 1, 1)),          # không đổi
           _row("00002", "Phường B Mới", box(1, 0, 2, 1)),      # đổi tên
           _row("00003", "Phường C", box(2, 0, 3, 2)),          # đổi ranh giới
           _row("00004", "Phường D", box(3, 0, 4, 1))]          # thêm mới
    p_old, p_new = tmp_path / "old.parquet", tmp_path / "new.parquet"
    pq.write_table(rows_to_table(old), p_old)
    pq.write_table(rows_to_table(new), p_new)
    return p_old, p_new


def test_phan_loai_dung_bon_nhom_thay_doi(paths):
    r = diff(*paths)
    assert r.added == ["00004"]
    assert r.removed == []
    assert r.renamed == [("00002", "Phường B", "Phường B Mới")]
    assert r.geometry_changed == ["00003"]
    assert r.unchanged == 1


def test_bat_duoc_xa_bi_xoa(tmp_path, paths):
    p_old, p_new = paths
    r = diff(p_new, p_old)
    assert r.removed == ["00004"]
