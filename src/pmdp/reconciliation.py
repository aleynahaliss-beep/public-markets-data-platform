"""Front-office vs back-office trade reconciliation."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from .config import Config


def reconcile(trades_clean: DataFrame, instruments_clean: DataFrame, settlements_clean: DataFrame,
              rejected_trade_ids: DataFrame, cfg: Config) -> DataFrame:
    """Full outer join on trade_id and classify every difference.

    Categories (first match wins):
      FO_REJECTED_BY_DQ        back office holds the trade, front office row failed a CRITICAL DQ rule
      MISSING_IN_FRONT_OFFICE  back-office record with no front-office trade at all
      MISSING_IN_BACK_OFFICE   live front-office trade with no settlement instruction
      QUANTITY_BREAK / AMOUNT_BREAK / SETTLE_DATE_BREAK
      MATCHED
    """
    mult = instruments_clean.select("instrument_id", "price_multiplier")
    fo = (trades_clean.filter(F.col("status") != "CANCELLED").join(mult, "instrument_id", "left")
          .select("trade_id", "counterparty_id", "instrument_id",
                  F.col("settle_date").alias("fo_settle_date"),
                  F.col("quantity").alias("fo_quantity"),
                  F.round(F.col("quantity") * F.col("price_multiplier") * F.col("price"), 2).alias("fo_amount")))
    bo = settlements_clean.select("trade_id", F.col("settle_date").alias("bo_settle_date"),
                                  F.col("quantity").alias("bo_quantity"), F.col("net_amount").alias("bo_amount"))
    rejected = rejected_trade_ids.select("trade_id").distinct().withColumn("_rejected", F.lit(True))

    j = fo.join(bo, "trade_id", "full_outer").join(rejected, "trade_id", "left")
    in_fo, in_bo = F.col("fo_quantity").isNotNull(), F.col("bo_quantity").isNotNull()
    both = in_fo & in_bo

    category = (F.when(~in_fo & in_bo & F.col("_rejected").isNotNull(), "FO_REJECTED_BY_DQ")
                 .when(~in_fo, "MISSING_IN_FRONT_OFFICE")
                 .when(~in_bo, "MISSING_IN_BACK_OFFICE")
                 .when(both & (F.col("fo_quantity") != F.col("bo_quantity")), "QUANTITY_BREAK")
                 .when(both & (F.abs(F.col("fo_amount") - F.col("bo_amount")) > cfg.recon_amount_tolerance),
                       "AMOUNT_BREAK")
                 .when(both & (F.col("fo_settle_date") != F.col("bo_settle_date")), "SETTLE_DATE_BREAK")
                 .otherwise("MATCHED"))
    return (j.withColumn("break_category", category)
             .withColumn("recon_status", F.when(F.col("break_category") == "MATCHED", "MATCHED").otherwise("BREAK"))
             .withColumn("amount_diff", F.round(F.col("fo_amount") - F.col("bo_amount"), 2))
             .select("trade_id", "counterparty_id", "instrument_id", "fo_quantity", "bo_quantity", "fo_amount",
                     "bo_amount", "amount_diff", "fo_settle_date", "bo_settle_date", "recon_status",
                     "break_category"))


def recon_summary(recon: DataFrame) -> DataFrame:
    total = recon.count()
    return (recon.groupBy("break_category").count()
            .withColumn("pct_of_records", F.round(100.0 * F.col("count") / F.lit(total), 2))
            .withColumnRenamed("count", "records").orderBy(F.desc("records")))
