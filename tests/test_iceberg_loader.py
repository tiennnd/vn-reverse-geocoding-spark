from datetime import date

import pytest

from vn_geo_load.iceberg_loader import load

pytestmark = pytest.mark.spark


def _seed(spark, table):
    """Bảng Iceberg thật, vì `load` dùng UPDATE — parquet thường không chạy được."""
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(
        f"""
        CREATE TABLE {table} (
            ward_code STRING, ward_full_name STRING, geom BINARY,
            valid_from DATE, valid_to DATE, is_current BOOLEAN,
            decree_ref STRING, source_release STRING
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        INSERT INTO {table} VALUES
        ('00001', 'Phường Cũ', CAST('x' AS BINARY),
         DATE '2026-04-30', DATE '9999-12-31', true, '30/2026/QH16', 'v5.0.0')
        """
    )


def test_dong_ban_cu_va_mo_ban_moi(spark, tmp_path):
    table = "local.db.dim_scd2_test"
    _seed(spark, table)

    new_rows = spark.createDataFrame(
        [("00001", "Phường Mới", bytearray(b"y"))],
        ["ward_code", "ward_full_name", "geom"],
    )
    parquet = str(tmp_path / "new.parquet")
    new_rows.write.mode("overwrite").parquet(parquet)

    stats = load(spark, parquet, table, date(2027, 1, 1), "99/2027/QH16", "v6.0.0")

    assert stats == {"closed": 1, "inserted": 1}
    rows = {r["is_current"]: r for r in spark.table(table).collect()}
    assert rows[False]["valid_to"] == date(2027, 1, 1)
    assert rows[True]["valid_from"] == date(2027, 1, 1)
    assert rows[True]["ward_full_name"] == "Phường Mới"
    assert rows[True]["decree_ref"] == "99/2027/QH16"


def test_hai_khoang_rap_khit_khong_ho_khong_de(spark, tmp_path):
    table = "local.db.dim_scd2_khit"
    _seed(spark, table)
    new_rows = spark.createDataFrame(
        [("00001", "Phường Mới", bytearray(b"y"))],
        ["ward_code", "ward_full_name", "geom"],
    )
    parquet = str(tmp_path / "new2.parquet")
    new_rows.write.mode("overwrite").parquet(parquet)
    load(spark, parquet, table, date(2027, 1, 1), "99/2027/QH16", "v6.0.0")

    # Đúng một bản ghi khớp với mọi ngày thử.
    for probe in ("2026-06-01", "2026-12-31", "2027-01-01", "2028-05-05"):
        n = spark.sql(
            f"SELECT count(*) c FROM {table} "
            f"WHERE DATE '{probe}' >= valid_from AND DATE '{probe}' < valid_to"
        ).collect()[0]["c"]
        assert n == 1, f"ngay {probe} khop {n} ban ghi, phai la 1"
