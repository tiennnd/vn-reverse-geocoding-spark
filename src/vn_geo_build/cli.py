"""Điểm vào dòng lệnh của nửa dựng dữ liệu."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from . import config
from .attribute_reader import read_units_json
from .dim_builder import DimRow, build_dim
from .geojson_reader import read_geojson_zip
from .source_fetcher import fetch_sources
from .validator import measure, validate

SCHEMA = pa.schema([
    ("ward_code", pa.string()),
    ("ward_name", pa.string()),
    ("ward_full_name", pa.string()),
    ("ward_name_en", pa.string()),
    ("ward_full_name_en", pa.string()),
    ("ward_code_name", pa.string()),
    ("ward_type", pa.string()),
    ("postal_code", pa.string()),
    ("province_code", pa.string()),
    ("province_name", pa.string()),
    ("province_full_name", pa.string()),
    ("province_name_en", pa.string()),
    ("province_postal_code_prefix", pa.string()),
    ("bbox", pa.binary()),
    ("geom", pa.binary()),
    ("area_km2", pa.float64()),
    ("valid_from", pa.date32()),
    ("valid_to", pa.date32()),
    ("is_current", pa.bool_()),
    ("decree_ref", pa.string()),
    ("source_release", pa.string()),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
])


def rows_to_table(rows: Sequence[DimRow]) -> pa.Table:
    cols = {name: [getattr(r, name) for r in rows] for name in SCHEMA.names}
    return pa.table(cols, schema=SCHEMA)


def build(cache_dir: Path, out_path: Path) -> int:
    zip_path, json_path = fetch_sources(cache_dir)
    features = list(read_geojson_zip(zip_path))
    provinces, wards = read_units_json(json_path)

    rows, report, province_geoms = build_dim(
        features, provinces, wards,
        valid_from=config.VALID_FROM,
        valid_to=config.FAR_FUTURE,
        decree_ref=config.DECREE_REF,
        source_release=config.RELEASE_TAG,
        ingested_at=datetime.now(timezone.utc),
    )

    issues = validate(
        rows, report, province_geoms,
        expected_ward_codes=set(wards),
        expected_province_codes=set(provinces),
    )
    blocking = [i for i in issues if not i.rule.startswith("canh_bao_")]
    metrics = measure(rows)

    report_path = out_path.with_suffix(".report.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "source_release": config.RELEASE_TAG,
                "n_wards": report.n_wards,
                "n_provinces": report.n_provinces,
                "n_geometries_fixed": report.n_geometries_fixed,
                "national_bbox": list(report.national_bbox),
                # Luôn ghi số đo, kể cả khi trong ngưỡng — đây là căn cứ
                # quyết định có cần snap_tolerance_m hay không (spec 6.3).
                "metrics": metrics,
                "issues": [{"rule": i.rule, "detail": i.detail} for i in issues],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    for i in issues:
        print(f"[{i.rule}] {i.detail}", file=sys.stderr)

    if blocking:
        print(f"KIEM DINH THAT BAI: {len(blocking)} loi. Khong xuat ban.", file=sys.stderr)
        return 1

    pq.write_table(rows_to_table(rows), out_path)
    print(f"OK: {report.n_wards} xa -> {out_path}")
    print(f"bao cao: {report_path}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(prog="vn-geo-build")
    ap.add_argument("--cache-dir", type=Path, default=Path(".cache"))
    ap.add_argument("--out", type=Path, default=Path("build/dim_vn_ward_gis.parquet"))
    args = ap.parse_args()
    return build(args.cache_dir, args.out)


if __name__ == "__main__":
    raise SystemExit(main())
