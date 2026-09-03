"""Chạy trên cụm để xác nhận Sedona hoạt động. Không phải là test tự động."""
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext


def main() -> None:
    spark = SedonaContext.create(SparkSession.builder.appName("sedona-smoke").getOrCreate())
    df = spark.sql(
        """
        SELECT ST_Contains(
                 ST_GeomFromWKT('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
                 ST_Point(5.0, 5.0)
               ) AS inside,
               ST_Contains(
                 ST_GeomFromWKT('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
                 ST_Point(50.0, 50.0)
               ) AS outside
        """
    )
    row = df.collect()[0]
    assert row["inside"] is True, "ST_Contains sai với điểm bên trong"
    assert row["outside"] is False, "ST_Contains sai với điểm bên ngoài"
    print("SEDONA OK")


if __name__ == "__main__":
    main()
