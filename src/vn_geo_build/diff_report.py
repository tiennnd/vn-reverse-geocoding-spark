"""So sánh hai phiên bản bảng chiều. Đầu ra dành cho người duyệt đọc."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.parquet as pq


@dataclass
class DiffReport:
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    renamed: list[tuple[str, str, str]] = field(default_factory=list)
    geometry_changed: list[str] = field(default_factory=list)
    unchanged: int = 0


def _index(path: Path) -> dict[str, tuple[str, str]]:
    """ward_code -> (ward_full_name, bam_cua_geometry)"""
    t = pq.read_table(path, columns=["ward_code", "ward_full_name", "geom"])
    d = t.to_pydict()
    return {
        code: (name, hashlib.sha256(geom).hexdigest())
        for code, name, geom in zip(d["ward_code"], d["ward_full_name"], d["geom"])
    }


def diff(old_parquet: Path, new_parquet: Path) -> DiffReport:
    old, new = _index(Path(old_parquet)), _index(Path(new_parquet))
    r = DiffReport()
    r.added = sorted(set(new) - set(old))
    r.removed = sorted(set(old) - set(new))

    for code in sorted(set(old) & set(new)):
        old_name, old_hash = old[code]
        new_name, new_hash = new[code]
        changed = False
        if old_name != new_name:
            r.renamed.append((code, old_name, new_name))
            changed = True
        if old_hash != new_hash:
            r.geometry_changed.append(code)
            changed = True
        if not changed:
            r.unchanged += 1
    return r


def format_report(r: DiffReport) -> str:
    lines = [
        "BAO CAO KHAC BIET GIUA HAI PHIEN BAN BANG CHIEU",
        "",
        f"  Them moi      : {len(r.added)}",
        f"  Xoa           : {len(r.removed)}",
        f"  Doi ten       : {len(r.renamed)}",
        f"  Doi ranh gioi : {len(r.geometry_changed)}",
        f"  Khong doi     : {r.unchanged}",
        "",
    ]
    if r.added:
        lines += ["Them moi:", *(f"  + {c}" for c in r.added), ""]
    if r.removed:
        lines += ["Xoa:", *(f"  - {c}" for c in r.removed), ""]
    if r.renamed:
        lines += ["Doi ten:", *(f"  ~ {c}: {a}  ->  {b}" for c, a, b in r.renamed), ""]
    if r.geometry_changed:
        lines += ["Doi ranh gioi:", *(f"  # {c}" for c in r.geometry_changed[:50])]
        if len(r.geometry_changed) > 50:
            lines.append(f"  ... va {len(r.geometry_changed) - 50} xa khac")
        lines.append("")
    return "\n".join(lines)
