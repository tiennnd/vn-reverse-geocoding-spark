from pathlib import Path

import pyarrow.parquet as pq

from vn_geo_build.cli import rows_to_table

FIX = Path(__file__).parent / "fixtures"


def _rows():
    from datetime import date, datetime, timezone

    from vn_geo_build.attribute_reader import read_units_json
    from vn_geo_build.dim_builder import build_dim
    from vn_geo_build.geojson_reader import read_geojson_zip

    features = list(read_geojson_zip(FIX / "mini_geojson.zip"))
    provinces, wards = read_units_json(FIX / "mini_units.json")
    rows, _, _ = build_dim(
        features, provinces, wards,
        valid_from=date(2026, 4, 30), valid_to=date(9999, 12, 31),
        decree_ref="30/2026/QH16", source_release="v5.0.0",
        ingested_at=datetime(2026, 9, 3, tzinfo=timezone.utc),
    )
    return rows


def test_parquet_giu_dung_kieu_du_lieu(tmp_path):
    table = rows_to_table(_rows())
    out = tmp_path / "dim.parquet"
    pq.write_table(table, out)

    back = pq.read_table(out)
    schema = {f.name: str(f.type) for f in back.schema}
    assert schema["geom"] == "binary"
    assert schema["bbox"] == "binary"
    assert schema["valid_from"] == "date32[day]"
    assert schema["is_current"] == "bool"
    assert schema["ingested_at"].startswith("timestamp[us")
    assert back.num_rows == 3


def test_cot_parquet_khop_dung_lucdo_spec():
    table = rows_to_table(_rows())
    assert table.column_names == [
        "ward_code", "ward_name", "ward_full_name", "ward_name_en",
        "ward_full_name_en", "ward_code_name", "ward_type", "postal_code",
        "province_code", "province_name", "province_full_name",
        "province_name_en", "province_postal_code_prefix",
        "bbox", "geom", "area_km2",
        "valid_from", "valid_to", "is_current", "decree_ref",
        "source_release", "ingested_at",
    ]
