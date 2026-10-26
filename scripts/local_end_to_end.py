"""Chạy toàn trình trên máy phát triển: tạo bảng Iceberg, nạp, kiểm chứng.

Đây là bản địa phương của Task 9 bước 5 và Task 10 bước 2 trong kế hoạch.
Trên cụm thì dùng spark-submit với iceberg_loader.py và verify_accuracy.py.

    python scripts/local_end_to_end.py --parquet build/dim_vn_ward_gis.parquet
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

BREW_JDK17 = Path("/opt/homebrew/opt/openjdk@17")
if "JAVA_HOME" not in os.environ and BREW_JDK17.exists():
    os.environ["JAVA_HOME"] = str(BREW_JDK17)
# Worker mặc định chạy `python3` trong PATH, có thể khác minor với driver
# (vd venv 3.11, hệ thống 3.13) và hỏng với PYTHON_VERSION_MISMATCH.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from sedona.spark import SedonaContext  # noqa: E402

from vn_geo_build import config  # noqa: E402
from vn_geo_load.iceberg_loader import load  # noqa: E402

DEFAULT_PACKAGES = ",".join([
    "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.1",
    "org.datasyslab:geotools-wrapper:1.9.1-33.5",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0",
])
EXTENSIONS = ",".join([
    "org.apache.sedona.sql.SedonaSqlExtensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
])

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  ward_code STRING, ward_name STRING, ward_full_name STRING,
  ward_name_en STRING, ward_full_name_en STRING, ward_code_name STRING,
  ward_type STRING, postal_code STRING,
  province_code STRING, province_name STRING, province_full_name STRING,
  province_name_en STRING, province_postal_code_prefix STRING,
  bbox BINARY, geom BINARY, area_km2 DOUBLE,
  valid_from DATE, valid_to DATE, is_current BOOLEAN,
  decree_ref STRING, source_release STRING, ingested_at TIMESTAMP
) USING iceberg
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default="build/dim_vn_ward_gis.parquet")
    ap.add_argument("--warehouse", default="build/warehouse")
    args = ap.parse_args()

    spark = SedonaContext.create(
        SedonaContext.builder()
        .master("local[4]").appName("vn-geo-local-e2e")
        .config("spark.jars.packages", os.environ.get("VN_GEO_SPARK_PACKAGES", DEFAULT_PACKAGES))
        .config("spark.sql.extensions", EXTENSIONS)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(Path(args.warehouse).resolve()))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    table = "local.dim.dim_vn_ward_gis"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.dim")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(DDL.format(table=table))

    stats = load(spark, args.parquet, table,
                 config.VALID_FROM, config.DECREE_REF, config.RELEASE_TAG)
    print(f"\nNAP: dong {stats['closed']} ban cu, chen {stats['inserted']} ban moi")

    # Catalog Iceberg kiểu `hadoop` không hỗ trợ view. Trên cụm dùng catalog
    # Hive/REST thì `CREATE VIEW dim.dim_vn_ward_gis_current` chạy bình thường;
    # ở đây thay bằng temp view để phần còn lại vẫn chạy được.
    try:
        spark.sql(f"CREATE OR REPLACE VIEW local.dim.dim_vn_ward_gis_current AS "
                  f"SELECT * FROM {table} WHERE is_current")
        print("VIEW: da tao view thuong")
    except Exception as e:
        spark.sql(f"SELECT * FROM {table} WHERE is_current") \
             .createOrReplaceTempView("dim_vn_ward_gis_current")
        print(f"VIEW: catalog cuc bo khong ho tro view ({type(e).__name__}), "
              f"dung temp view thay the")

    from verify_accuracy import kiem_tra_diem_dai_dien
    print()
    ti_le = kiem_tra_diem_dai_dien(spark, table)

    print("\nTRA THU TOA DO THAT (qua duong Sedona):")
    pts = spark.createDataFrame(
        [("Ho Guom, Ha Noi", 21.0287, 105.8524, date(2026, 6, 1)),
         ("Cho Ben Thanh, TP.HCM", 10.7725, 106.6980, date(2026, 6, 1)),
         ("Mui Ca Mau", 8.6200, 104.7200, date(2026, 6, 1)),
         ("Dao Truong Sa Lon", 8.6440, 111.9200, date(2026, 6, 1)),
         ("Bien Dong ngoai khoi", 15.0000, 113.0000, date(2026, 6, 1)),
         ("Giao dich nam 2024", 21.0287, 105.8524, date(2024, 1, 1)),
         ("Toa do dao nguoc", 105.8524, 21.0287, date(2026, 6, 1)),
         ("Null Island", 0.0, 0.0, date(2026, 6, 1))],
        ["label", "lat", "lon", "txn_date"])

    from vn_geo.attach import attach_vn_address
    attach_vn_address(pts, lat_col="lat", lon_col="lon",
                      as_of_col="txn_date", dim_table=table) \
        .select("label", "addr_ward_full_name", "addr_province_full_name",
                "addr_postal_code", "addr_match_status", "addr_geo_ref_is_exact") \
        .show(20, truncate=False)

    spark.stop()
    raise SystemExit(0 if ti_le == 1.0 else 1)


if __name__ == "__main__":
    main()
