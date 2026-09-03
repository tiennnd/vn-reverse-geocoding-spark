"""Chín luật kiểm định. Danh sách trả về rỗng nghĩa là được phép xuất bản."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Sequence

from shapely import wkb
from shapely.geometry.base import BaseGeometry
from shapely.strtree import STRtree

from . import config
from .dim_builder import BuildReport, DimRow


@dataclass(frozen=True)
class ValidationIssue:
    rule: str
    detail: str


def validate(
    rows: Sequence[DimRow],
    report: BuildReport,
    province_geoms: dict[str, BaseGeometry],
    *,
    expected_ward_codes: set[str],
    expected_province_codes: set[str],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    geoms = {r.ward_code: wkb.loads(r.geom) for r in rows}

    # 1. Toàn vẹn tham chiếu: hai nguồn phải khớp nhau, không so với hằng số
    have = {r.ward_code for r in rows}
    if have != expected_ward_codes:
        thieu = sorted(expected_ward_codes - have)[:10]
        thua = sorted(have - expected_ward_codes)[:10]
        issues.append(ValidationIssue(
            "toan_ven_tham_chieu",
            f"thieu hinh hoc: {thieu}; thua hinh hoc: {thua}",
        ))
    have_p = {r.province_code for r in rows}
    if not have_p <= expected_province_codes:
        issues.append(ValidationIssue(
            "toan_ven_tham_chieu",
            f"ma tinh la: {sorted(have_p - expected_province_codes)[:10]}",
        ))

    # 2. Hình học hợp lệ
    bad = [c for c, g in geoms.items() if not g.is_valid]
    if bad:
        issues.append(ValidationIssue("hinh_hoc_hop_le", f"{len(bad)} xa: {bad[:10]}"))

    # 3. Không polygon rỗng hoặc diện tích 0
    empty = [c for c, g in geoms.items() if g.is_empty or g.area == 0]
    if empty:
        issues.append(ValidationIssue("polygon_rong", f"{len(empty)} xa: {empty[:10]}"))

    # 4. Tổng diện tích
    total = sum(r.area_km2 for r in rows)
    lo = config.TOTAL_AREA_KM2 * (1 - config.AREA_TOLERANCE)
    hi = config.TOTAL_AREA_KM2 * (1 + config.AREA_TOLERANCE)
    if not lo <= total <= hi:
        issues.append(ValidationIssue(
            "tong_dien_tich", f"{total:,.0f} km2, ngoai khoang [{lo:,.0f}, {hi:,.0f}]"
        ))

    # 5. Chồng lấn — dùng STRtree để không phải so mọi cặp
    codes = list(geoms)
    tree = STRtree([geoms[c] for c in codes])
    overlap = 0.0
    total_area_deg = sum(g.area for g in geoms.values()) or 1.0
    for i, c in enumerate(codes):
        for j in tree.query(geoms[c]):
            if j <= i:
                continue
            inter = geoms[c].intersection(geoms[codes[j]])
            if not inter.is_empty:
                overlap += inter.area
    if overlap / total_area_deg > config.OVERLAP_TOLERANCE:
        issues.append(ValidationIssue(
            "chong_lan", f"ti le chong lan {overlap / total_area_deg:.5%}"
        ))

    # 6. Khe hở trong từng tỉnh
    by_province: dict[str, list[BaseGeometry]] = defaultdict(list)
    for r in rows:
        by_province[r.province_code].append(geoms[r.ward_code])
    for pcode, ws in by_province.items():
        pg = province_geoms.get(pcode)
        if pg is None:
            continue
        union_area = sum(w.area for w in ws)
        if pg.area <= 0:
            continue
        gap = (pg.area - union_area) / pg.area
        if gap > config.GAP_TOLERANCE:
            issues.append(ValidationIssue("khe_ho", f"tinh {pcode}: khe ho {gap:.5%}"))

    # 7. Nhất quán chéo GIS và thuộc tính
    for r in rows:
        pg = province_geoms.get(r.province_code)
        if pg is None:
            continue
        if not pg.contains(geoms[r.ward_code].representative_point()):
            issues.append(ValidationIssue(
                "nhat_quan_cheo",
                f"xa {r.ward_code} khong nam trong tinh {r.province_code}",
            ))

    # 8. Toàn vẹn SCD2: không chồng lấn khoảng thời gian
    spans: dict[str, list[tuple]] = defaultdict(list)
    for r in rows:
        spans[r.ward_code].append((r.valid_from, r.valid_to))
    for code, ss in spans.items():
        ss.sort()
        for (f1, t1), (f2, _) in zip(ss, ss[1:]):
            if f2 < t1:
                issues.append(ValidationIssue(
                    "toan_ven_scd2", f"xa {code}: khoang {f1}-{t1} de len {f2}"
                ))

    # 9. Đúng một bản is_current cho mỗi xã
    for code, rs in _group_by_ward(rows).items():
        n_current = sum(1 for r in rs if r.is_current)
        if n_current != 1:
            issues.append(ValidationIssue(
                "toan_ven_scd2", f"xa {code} co {n_current} ban is_current"
            ))

    # 10. Số lượng hình học đã phải sửa — cảnh báo, không chặn
    if report.n_geometries_fixed > 0:
        issues.append(ValidationIssue(
            "canh_bao_hinh_hoc_da_sua",
            f"{report.n_geometries_fixed} hinh hoc phai sua bang make_valid",
        ))

    return issues


def _group_by_ward(rows: Sequence[DimRow]) -> dict[str, list[DimRow]]:
    out: dict[str, list[DimRow]] = defaultdict(list)
    for r in rows:
        out[r.ward_code].append(r)
    return out
