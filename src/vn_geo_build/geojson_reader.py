"""Đọc file zip GeoJSON của repo nguồn thành các bản ghi phẳng."""
from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry


@dataclass(frozen=True)
class GeoFeature:
    code: str
    level: str  # "province" | "ward"
    province_code: str
    name: str
    name_en: str
    full_name: str
    full_name_en: str
    code_name: str
    postal_code: str
    postal_code_prefix: str
    area_km2: float
    gis_server_id: str
    geometry: BaseGeometry


def province_code_from_dir(dir_name: str) -> str:
    """`01_ha_noi` -> `01`. Cắt ở gạch dưới ĐẦU TIÊN vì có tên như
    `19_tinh_quang_tri`."""
    return dir_name.split("_", 1)[0]


def _to_feature(raw: bytes, level: str, province_code: str) -> GeoFeature:
    doc = json.loads(raw)
    feat = doc["features"][0]
    p = feat["properties"]
    return GeoFeature(
        code=p["code"],
        level=level,
        province_code=province_code,
        name=p["name"],
        name_en=p.get("nameEn", ""),
        full_name=p.get("fullName", ""),
        full_name_en=p.get("fullNameEn", ""),
        code_name=p.get("codeName", ""),
        postal_code=p.get("postalCode", ""),
        postal_code_prefix=p.get("postalCodePrefix", ""),
        area_km2=float(p.get("areaKm2") or 0.0),
        gis_server_id=p.get("gisServerId", ""),
        geometry=shape(feat["geometry"]),
    )


def read_geojson_zip(zip_path: Path) -> Iterator[GeoFeature]:
    """Sinh ra GeoFeature cho mọi tỉnh và xã trong file zip.

    Bố cục mong đợi:
        geojson/{thu_muc_tinh}/{thu_muc_tinh}.geojson
        geojson/{thu_muc_tinh}/wards/{ma_xa}_{slug}.geojson
    """
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            if not name.endswith(".geojson"):
                continue
            parts = name.split("/")
            if len(parts) < 3:
                continue
            province_code = province_code_from_dir(parts[1])
            level = "ward" if "wards" in parts else "province"
            yield _to_feature(z.read(name), level, province_code)
