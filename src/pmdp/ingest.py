"""Landing -> staging: read raw CSV feeds with explicit schemas (no schema inference)."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (DateType, DoubleType, StringType, StructField, StructType)

S, D, N = StringType(), DateType(), DoubleType()

SCHEMAS: dict[str, StructType] = {
    "instruments": StructType([
        StructField("instrument_id", S), StructField("isin", S), StructField("ticker", S),
        StructField("instrument_name", S), StructField("asset_class", S), StructField("currency", S),
        StructField("country", S), StructField("sector", S), StructField("price_multiplier", N),
        StructField("maturity_date", D)]),
    "counterparties": StructType([
        StructField("counterparty_id", S), StructField("counterparty_name", S), StructField("lei", S),
        StructField("rating", S), StructField("country", S), StructField("csa_flag", S),
        StructField("threshold_usd", N), StructField("mta_usd", N), StructField("rounding_usd", N)]),
    "prices": StructType([
        StructField("price_date", D), StructField("instrument_id", S), StructField("close_price", N),
        StructField("price_currency", S)]),
    "fx_rates": StructType([
        StructField("rate_date", D), StructField("currency", S), StructField("usd_per_unit", N)]),
    "trades": StructType([
        StructField("trade_id", S), StructField("trade_date", D), StructField("settle_date", D),
        StructField("instrument_id", S), StructField("counterparty_id", S), StructField("side", S),
        StructField("quantity", N), StructField("price", N), StructField("currency", S),
        StructField("status", S)]),
    "settlements": StructType([
        StructField("trade_id", S), StructField("settle_date", D), StructField("quantity", N),
        StructField("net_amount", N), StructField("currency", S), StructField("status", S)]),
    "collateral": StructType([
        StructField("counterparty_id", S), StructField("collateral_as_of", D),
        StructField("collateral_held_usd", N)]),
}


def read_feed(spark: SparkSession, raw_dir: str | Path, name: str) -> DataFrame:
    """Read one feed. `ingest_seq` preserves file order so 'first occurrence wins' is deterministic."""
    df = (spark.read.option("header", True).option("mode", "PERMISSIVE")
          .option("dateFormat", "yyyy-MM-dd").schema(SCHEMAS[name])
          .csv(str(Path(raw_dir) / f"{name}.csv")))
    return df.coalesce(1).withColumn("ingest_seq", F.monotonically_increasing_id())


def read_all(spark: SparkSession, raw_dir: str | Path) -> dict[str, DataFrame]:
    return {name: read_feed(spark, raw_dir, name) for name in SCHEMAS}
