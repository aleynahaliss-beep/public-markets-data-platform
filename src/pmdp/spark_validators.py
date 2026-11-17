"""Native Spark implementations of identifier checks (no Python UDFs -> no Python workers needed).

`validators.is_valid_isin` is the readable reference implementation; the unit tests assert that the
Spark expression below agrees with it on valid, mutated and malformed identifiers.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F

ISIN_HELPERS = ["_isin_digits", "_isin_luhn"]


def add_isin_helpers(df: DataFrame, col: str = "isin") -> DataFrame:
    """Add `_isin_digits` (letters expanded to two digits) and `_isin_luhn` (Luhn sum) columns."""
    digits = F.expr(f"""
        concat_ws('', transform(sequence(1, 11), i ->
            CASE WHEN substring(`{col}`, i, 1) RLIKE '^[A-Z]$'
                 THEN cast(ascii(substring(`{col}`, i, 1)) - 55 AS string)
                 ELSE substring(`{col}`, i, 1) END))""")
    luhn = F.expr("""
        aggregate(
            transform(sequence(0, length(_isin_digits) - 1), i ->
                CASE WHEN i % 2 = 0
                     THEN (cast(substring(_isin_digits, length(_isin_digits) - i, 1) AS int) * 2) DIV 10
                        + (cast(substring(_isin_digits, length(_isin_digits) - i, 1) AS int) * 2) % 10
                     ELSE cast(substring(_isin_digits, length(_isin_digits) - i, 1) AS int) END),
            CAST(0 AS BIGINT), (acc, x) -> acc + x)""")
    return df.withColumn("_isin_digits", digits).withColumn("_isin_luhn", luhn)


def isin_is_valid(col: str = "isin") -> Column:
    """True only for a well-formed ISIN with a correct ISO 6166 check digit (NULL input -> False)."""
    well_formed = F.col(col).rlike("^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
    check_ok = ((F.lit(10) - F.col("_isin_luhn") % 10) % 10) == F.substring(F.col(col), 12, 1).cast("int")
    return F.coalesce(well_formed & check_ok, F.lit(False))
