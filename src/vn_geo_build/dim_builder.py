"""Ghép GeoJSON và thuộc tính hành chính thành các dòng bảng chiều."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Sequence

import shapely
from shapely import wkb
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry

from .attribute_reader import ProvinceAttrs, WardAttrs
from .geojson_reader import GeoFeature


@dataclass(frozen=True)
class DimRow:
    ward_code: str
    ward_name: str
    ward_full_name: str
    ward_name_en: str
    ward_full_name_en: str
    ward_code_name: str
    ward_type: str
    postal_code: str
    province_code: str
    province_name: str
    province_full_name: str
    province_name_en: str
    province_postal_code_prefix: str
    bbox: bytes
    geom: bytes
    area_km2: float
    valid_from: date
    valid_to: date
    is_current: bool
    decree_ref: str
    source_release: str
    ingested_at: datetime


@dataclass(frozen=True)
class BuildReport:
    n_wards: int
    n_provinces: int
    n_geometries_fixed: int
    national_bbox: tuple[float, float, float, float]
    # (ma_xa, ma_tinh_theo_thu_muc_geojson, ma_tinh_theo_json_thuoc_tinh)
    province_code_mismatches: tuple[tuple[str, str, str], ...] = ()


def _ensure_valid(geom: BaseGeometry) -> tuple[BaseGeometry, bool]:
    """Trả về (hinh_hoc_hop_le, da_phai_sua)."""
    if geom.is_valid:
        return geom, False
    return shapely.make_valid(geom), True


def build_dim(
    features: Iterable[GeoFeature],
    provinces: dict[str, ProvinceAttrs],
    wards: dict[str, WardAttrs],
    *,
    valid_from: date,
    valid_to: date,
    decree_ref: str,
    source_release: str,
    ingested_at: datetime,
) -> tuple[list[DimRow], BuildReport, dict[str, BaseGeometry]]:
    """Dựng danh sách DimRow cấp xã, kèm báo cáo và geometry cấp tỉnh.

    Ném KeyError ngay khi gặp một xã không có thuộc tính tương ứng —
    nửa dựng dữ liệu hỏng là dừng, không đoán bù.
    """
    feats: Sequence[GeoFeature] = list(features)
    province_geoms: dict[str, BaseGeometry] = {}
    rows: list[DimRow] = []
    mismatches: list[tuple[str, str, str]] = []
    n_fixed = 0
    minx = miny = float("inf")
    maxx = maxy = float("-inf")

    for f in feats:
        geom, fixed = _ensure_valid(f.geometry)
        n_fixed += int(fixed)
        b = geom.bounds
        minx, miny = min(minx, b[0]), min(miny, b[1])
        maxx, maxy = max(maxx, b[2]), max(maxy, b[3])

        if f.level == "province":
            province_geoms[f.code] = geom
            continue

        wa = wards.get(f.code)
        if wa is None:
            raise KeyError(f"xa {f.code} co hinh hoc nhung khong co thuoc tinh")
        pa = provinces.get(wa.province_code)
        if pa is None:
            raise KeyError(f"tinh {wa.province_code} khong co thuoc tinh")

        # Đối chiếu chéo hai nguồn độc lập: thư mục trong zip GeoJSON và
        # trường ProvinceCode trong JSON thuộc tính. Lệch nhau nghĩa là
        # hình học đã bị ghép nhầm thuộc tính.
        if f.province_code != wa.province_code:
            mismatches.append((f.code, f.province_code, wa.province_code))

        rows.append(
            DimRow(
                ward_code=wa.ward_code,
                ward_name=wa.ward_name,
                ward_full_name=wa.ward_full_name,
                ward_name_en=wa.ward_name_en,
                ward_full_name_en=wa.ward_full_name_en,
                ward_code_name=wa.ward_code_name,
                ward_type=wa.ward_type,
                postal_code=wa.postal_code,
                province_code=pa.province_code,
                province_name=pa.province_name,
                province_full_name=pa.province_full_name,
                province_name_en=pa.province_name_en,
                province_postal_code_prefix=pa.province_postal_code_prefix,
                bbox=wkb.dumps(box(*geom.bounds)),
                geom=wkb.dumps(geom),
                area_km2=f.area_km2,
                valid_from=valid_from,
                valid_to=valid_to,
                is_current=True,
                decree_ref=decree_ref,
                source_release=source_release,
                ingested_at=ingested_at,
            )
        )

    report = BuildReport(
        n_wards=len(rows),
        n_provinces=len(province_geoms),
        n_geometries_fixed=n_fixed,
        national_bbox=(minx, miny, maxx, maxy),
        province_code_mismatches=tuple(mismatches),
    )
    return rows, report, province_geoms
