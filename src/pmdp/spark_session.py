"""SparkSession factory with settings that keep local runs deterministic and quiet."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "pmdp", shuffle_partitions: int = 4) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        # Spark 4 turns ANSI mode on by default; keep behaviour identical across 3.5 / 4.x.
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
