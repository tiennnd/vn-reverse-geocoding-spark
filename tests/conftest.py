"""SparkSession dùng chung cho các test có nhãn `spark`.

Toạ độ gói lấy từ biến môi trường `VN_GEO_SPARK_PACKAGES` nếu có, mặc định
là bộ đã xác minh trên máy phát triển (xem docs/environment.md mục A).
Khi biết phiên bản của cụm thì đặt biến đó, không sửa file này.
"""
import os
from pathlib import Path

import pytest

DEFAULT_PACKAGES = ",".join([
    "org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.9.1",
    "org.datasyslab:geotools-wrapper:1.9.1-33.5",
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0",
])

EXTENSIONS = ",".join([
    "org.apache.sedona.sql.SedonaSqlExtensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
])

# JDK cài bằng brew là keg-only nên không nằm trong PATH hệ thống.
BREW_JDK17 = Path("/opt/homebrew/opt/openjdk@17")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    if "JAVA_HOME" not in os.environ and BREW_JDK17.exists():
        os.environ["JAVA_HOME"] = str(BREW_JDK17)

    pytest.importorskip("pyspark")
    sedona_spark = pytest.importorskip("sedona.spark")

    warehouse = tmp_path_factory.mktemp("iceberg_warehouse")
    builder = (
        sedona_spark.SedonaContext.builder()
        .master("local[2]")
        .appName("vn-geo-tests")
        .config("spark.jars.packages", os.environ.get("VN_GEO_SPARK_PACKAGES", DEFAULT_PACKAGES))
        .config("spark.sql.extensions", EXTENSIONS)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(warehouse))
    )
    session = sedona_spark.SedonaContext.create(builder.getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
