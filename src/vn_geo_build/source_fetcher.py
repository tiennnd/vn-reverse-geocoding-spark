"""Tải file nguồn từ release đã ghim và đối chiếu băm SHA-256."""
from __future__ import annotations

import hashlib
from pathlib import Path

import requests

from . import config


class ChecksumMismatch(RuntimeError):
    """Nội dung tải về không khớp băm mong đợi."""


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(url: str, dest: Path, expected_sha256: str) -> Path:
    """Tải `url` về `dest`. Bỏ qua nếu `dest` đã có và đúng băm.

    Ném ChecksumMismatch và xoá file hỏng nếu băm sai — không bao giờ
    để lại một file sai trên đĩa cho lần chạy sau nhặt nhầm.
    """
    if dest.exists() and _sha256(dest) == expected_sha256:
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with dest.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)

    actual = _sha256(dest)
    if actual != expected_sha256:
        dest.unlink(missing_ok=True)
        raise ChecksumMismatch(
            f"{url}\n  mong doi: {expected_sha256}\n  thuc te : {actual}"
        )
    return dest


def fetch_sources(cache_dir: Path) -> tuple[Path, Path]:
    """Tải cả hai file nguồn. Trả về (duong_dan_zip, duong_dan_json)."""
    zip_name = "vn_provinces_wards_geojson.zip"
    json_name = "full_json_generated_data_vn_units.json"
    zip_path = fetch(
        config.GEOJSON_ZIP_URL, cache_dir / zip_name, config.SOURCE_CHECKSUMS[zip_name]
    )
    json_path = fetch(
        config.UNITS_JSON_URL, cache_dir / json_name, config.SOURCE_CHECKSUMS[json_name]
    )
    return zip_path, json_path
