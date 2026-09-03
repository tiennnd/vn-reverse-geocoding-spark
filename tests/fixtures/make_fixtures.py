"""Cắt một fixture nhỏ từ nguồn thật. Chạy tay khi đổi RELEASE_TAG.

    python tests/fixtures/make_fixtures.py
"""
import json
import zipfile
from pathlib import Path

from vn_geo_build.config import SOURCE_CHECKSUMS, GEOJSON_ZIP_URL, UNITS_JSON_URL
from vn_geo_build.source_fetcher import fetch

HERE = Path(__file__).parent
KEEP_PROVINCES = {"01", "79"}          # Hà Nội, TP. Hồ Chí Minh
KEEP_WARDS = {"00004", "00008", "25747"}


def main() -> None:
    cache = HERE / ".cache"
    zip_path = fetch(GEOJSON_ZIP_URL, cache / "src.zip",
                     SOURCE_CHECKSUMS["vn_provinces_wards_geojson.zip"])
    json_path = fetch(UNITS_JSON_URL, cache / "src.json",
                      SOURCE_CHECKSUMS["full_json_generated_data_vn_units.json"])

    src = zipfile.ZipFile(zip_path)
    with zipfile.ZipFile(HERE / "mini_geojson.zip", "w", zipfile.ZIP_DEFLATED) as out:
        for name in src.namelist():
            parts = name.split("/")
            if len(parts) < 3:
                continue
            prov = parts[1].split("_", 1)[0]
            if prov not in KEEP_PROVINCES:
                continue
            is_ward = "wards" in parts
            if is_ward and parts[-1].split("_", 1)[0] not in KEEP_WARDS:
                continue
            out.writestr(name, src.read(name))

    units = json.loads(json_path.read_text(encoding="utf-8"))
    mini = []
    for p in units:
        if p["Code"] not in KEEP_PROVINCES:
            continue
        p = dict(p)
        p["Wards"] = [w for w in p["Wards"] if w["Code"] in KEEP_WARDS]
        mini.append(p)
    (HERE / "mini_units.json").write_text(
        json.dumps(mini, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print(f"fixture: {sum(len(p['Wards']) for p in mini)} xa, {len(mini)} tinh")


if __name__ == "__main__":
    main()
