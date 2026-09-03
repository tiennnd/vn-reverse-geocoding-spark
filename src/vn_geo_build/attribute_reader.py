"""Đọc thuộc tính hành chính từ full_json_generated_data_vn_units.json."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProvinceAttrs:
    province_code: str
    province_name: str
    province_name_en: str
    province_full_name: str
    province_full_name_en: str
    province_code_name: str
    province_postal_code_prefix: str
    province_type: str


@dataclass(frozen=True)
class WardAttrs:
    ward_code: str
    province_code: str
    ward_name: str
    ward_name_en: str
    ward_full_name: str
    ward_full_name_en: str
    ward_code_name: str
    ward_type: str
    postal_code: str


def read_units_json(path: Path) -> tuple[dict[str, ProvinceAttrs], dict[str, WardAttrs]]:
    """Trả về (theo_ma_tinh, theo_ma_xa)."""
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    provinces: dict[str, ProvinceAttrs] = {}
    wards: dict[str, WardAttrs] = {}

    for p in doc:
        provinces[p["Code"]] = ProvinceAttrs(
            province_code=p["Code"],
            province_name=p["Name"],
            province_name_en=p.get("NameEn", ""),
            province_full_name=p.get("FullName", ""),
            province_full_name_en=p.get("FullNameEn", ""),
            province_code_name=p.get("CodeName", ""),
            province_postal_code_prefix=p.get("PostalCodePrefix", ""),
            province_type=p.get("AdministrativeUnitShortName", ""),
        )
        for w in p.get("Wards", []):
            wards[w["Code"]] = WardAttrs(
                ward_code=w["Code"],
                province_code=w.get("ProvinceCode", p["Code"]),
                ward_name=w["Name"],
                ward_name_en=w.get("NameEn", ""),
                ward_full_name=w.get("FullName", ""),
                ward_full_name_en=w.get("FullNameEn", ""),
                ward_code_name=w.get("CodeName", ""),
                ward_type=w.get("AdministrativeUnitShortName", ""),
                postal_code=w.get("PostalCode", ""),
            )
    return provinces, wards
