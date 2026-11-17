"""Business data-quality rules per dataset.

Each dataset has
  * `prepare_*`  - adds helper columns (window ranks, look-ups against validated masters),
  * `*_rules`    - the declarative rule list (also used to generate the rule matrix in the docs).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

from .config import (ALLOWED_CURRENCIES, ASSET_CLASSES, TRADE_SIDES, TRADE_STATUSES, Config)
from .data_quality import (ACCURACY, COMPLETENESS, CONSISTENCY, CRITICAL, INTEGRITY, TIMELINESS,
                           UNIQUENESS, VALIDITY, WARNING, Rule)
from .spark_validators import ISIN_HELPERS, add_isin_helpers, isin_is_valid


def _blank(col: str):
    return F.col(col).isNull() | (F.trim(F.col(col)) == "")


# ------------------------------------------------------------------------------------------------
# Instruments
# ------------------------------------------------------------------------------------------------
def prepare_instruments(df: DataFrame):
    df = (df.withColumn("_id_rank", F.row_number().over(Window.partitionBy("instrument_id").orderBy("ingest_seq")))
            .withColumn("_isin_rank", F.row_number().over(Window.partitionBy("isin").orderBy("ingest_seq"))))
    return add_isin_helpers(df), ["_id_rank", "_isin_rank", *ISIN_HELPERS]


def instrument_rules(cfg: Config) -> list[Rule]:
    d = "instruments"
    return [
        Rule("INS_001", d, "Instrument ID is populated", COMPLETENESS, CRITICAL,
             lambda: _blank("instrument_id"), "Primary key - without it nothing downstream can join."),
        Rule("INS_002", d, "Instrument ID is unique", UNIQUENESS, CRITICAL,
             lambda: F.col("_id_rank") > 1, "Duplicate keys double-count positions and exposures."),
        Rule("INS_003", d, "ISIN has valid format and ISO 6166 check digit", VALIDITY, CRITICAL,
             lambda: ~isin_is_valid("isin"),
             "A mistyped ISIN breaks look-ups against vendor and regulatory feeds."),
        Rule("INS_004", d, "ISIN is unique across instruments", UNIQUENESS, CRITICAL,
             lambda: (F.col("_isin_rank") > 1) & F.col("isin").isNotNull(),
             "Two instruments sharing an ISIN make security identification ambiguous."),
        Rule("INS_005", d, "Currency is populated", COMPLETENESS, CRITICAL,
             lambda: _blank("currency"), "Currency drives FX conversion of every price and trade."),
        Rule("INS_006", d, "Currency is in the supported ISO 4217 list", VALIDITY, CRITICAL,
             lambda: ~F.col("currency").isin(*ALLOWED_CURRENCIES), "Unsupported currency has no FX rate."),
        Rule("INS_007", d, "Asset class is a recognised value", VALIDITY, CRITICAL,
             lambda: ~F.col("asset_class").isin(*ASSET_CLASSES), "Add-on factors and risk buckets key on asset class."),
        Rule("INS_008", d, "Sector is populated", COMPLETENESS, WARNING,
             lambda: _blank("sector"), "Needed for concentration reporting; not blocking for pricing."),
        Rule("INS_009", d, "Bonds carry a future maturity date", CONSISTENCY, CRITICAL,
             lambda: (F.col("asset_class") == "BOND")
             & (F.col("maturity_date").isNull() | (F.col("maturity_date") <= F.lit(cfg.as_of))),
             "Matured or undated bonds should not be live in the master."),
        Rule("INS_010", d, "Price multiplier is positive", VALIDITY, CRITICAL,
             lambda: F.col("price_multiplier") <= 0, "A zero or negative multiplier zeroes out or flips valuations."),
    ]


# ------------------------------------------------------------------------------------------------
# Counterparties
# ------------------------------------------------------------------------------------------------
def prepare_counterparties(df: DataFrame):
    df = df.withColumn("_id_rank", F.row_number().over(Window.partitionBy("counterparty_id").orderBy("ingest_seq")))
    return df, ["_id_rank"]


def counterparty_rules(cfg: Config) -> list[Rule]:
    d = "counterparties"
    return [
        Rule("CPT_001", d, "Counterparty ID is populated and unique", UNIQUENESS, CRITICAL,
             lambda: _blank("counterparty_id") | (F.col("_id_rank") > 1), "Primary key of the netting-set owner."),
        Rule("CPT_002", d, "LEI is 20 upper-case alphanumeric characters", VALIDITY, WARNING,
             lambda: ~F.col("lei").rlike("^[A-Z0-9]{20}$"), "LEI is required for regulatory reporting."),
        Rule("CPT_003", d, "Credit rating is populated", COMPLETENESS, WARNING,
             lambda: _blank("rating"), "Rating drives limits and reporting buckets."),
        Rule("CPT_004", d, "csa_flag is Y or N", VALIDITY, CRITICAL,
             lambda: ~F.col("csa_flag").isin("Y", "N"), "Determines whether margining applies."),
        Rule("CPT_005", d, "CSA counterparties carry threshold, MTA and rounding", COMPLETENESS, CRITICAL,
             lambda: (F.col("csa_flag") == "Y")
             & (F.col("threshold_usd").isNull() | F.col("mta_usd").isNull() | F.col("rounding_usd").isNull()),
             "Margin calls cannot be computed without CSA terms."),
    ]


# ------------------------------------------------------------------------------------------------
# Prices
# ------------------------------------------------------------------------------------------------
def prepare_prices(df: DataFrame, cfg: Config, instrument_ids: DataFrame):
    n = cfg.stale_price_days
    w = Window.partitionBy("instrument_id").orderBy("price_date", "ingest_seq")
    frame = w.rowsBetween(-(n - 1), 0)
    ids = instrument_ids.select("instrument_id").where(F.col("instrument_id").isNotNull()).distinct() \
        .withColumn("_ins_ok", F.lit(True))
    df = (df.withColumn("_dup_rank", F.row_number().over(
                Window.partitionBy("instrument_id", "price_date").orderBy("ingest_seq")))
            .withColumn("_prev", F.lag("close_price").over(w))
            .withColumn("_next", F.lead("close_price").over(w))
            .withColumn("_wmin", F.min("close_price").over(frame))
            .withColumn("_wmax", F.max("close_price").over(frame))
            .withColumn("_wcnt", F.count("close_price").over(frame))
            .join(F.broadcast(ids), "instrument_id", "left"))
    return df, ["_dup_rank", "_prev", "_next", "_wmin", "_wmax", "_wcnt", "_ins_ok"]


def price_rules(cfg: Config) -> list[Rule]:
    d, t, n = "prices", cfg.price_spike_threshold, cfg.stale_price_days

    def spike() -> "F.Column":
        r_prev = F.col("close_price") / F.col("_prev")
        r_next = F.col("close_price") / F.col("_next")
        valid = (F.col("close_price") > 0) & (F.col("_prev") > 0) & (F.col("_next") > 0)
        return valid & (((r_prev > 1 + t) & (r_next > 1 + t)) | ((r_prev < 1 - t) & (r_next < 1 - t)))

    return [
        Rule("PRC_001", d, "Close price is present and positive", VALIDITY, CRITICAL,
             lambda: F.col("close_price").isNull() | (F.col("close_price") <= 0),
             "Zero / negative prices corrupt returns and valuations."),
        Rule("PRC_002", d, "Price references a known instrument", INTEGRITY, CRITICAL,
             lambda: F.col("_ins_ok").isNull(), "Orphan prices cannot be attributed to a security."),
        Rule("PRC_003", d, "One price per instrument per date", UNIQUENESS, CRITICAL,
             lambda: F.col("_dup_rank") > 1, "Duplicates distort return series and VaR."),
        Rule("PRC_004", d, f"No isolated price spike (> {int(t * 100)}% move that reverses)", ACCURACY, CRITICAL,
             spike, "Bad ticks create phantom tail losses in historical-simulation VaR."),
        Rule("PRC_005", d, f"No stale price ({n} identical closes in a row)", TIMELINESS, WARNING,
             lambda: (F.col("_wcnt") == n) & (F.col("_wmin") == F.col("_wmax")),
             "A frozen price usually means a dead feed, not a flat market."),
    ]


# ------------------------------------------------------------------------------------------------
# FX
# ------------------------------------------------------------------------------------------------
def prepare_fx(df: DataFrame):
    df = df.withColumn("_dup_rank", F.row_number().over(
        Window.partitionBy("rate_date", "currency").orderBy("ingest_seq")))
    return df, ["_dup_rank"]


def fx_rules(cfg: Config) -> list[Rule]:
    d = "fx_rates"
    return [
        Rule("FXR_001", d, "FX rate is positive", VALIDITY, CRITICAL,
             lambda: F.col("usd_per_unit").isNull() | (F.col("usd_per_unit") <= 0), "Non-positive rate."),
        Rule("FXR_002", d, "One rate per currency per date", UNIQUENESS, CRITICAL,
             lambda: F.col("_dup_rank") > 1, "Duplicate rates make conversions non-deterministic."),
    ]


# ------------------------------------------------------------------------------------------------
# Trades
# ------------------------------------------------------------------------------------------------
def prepare_trades(df: DataFrame, instruments_clean: DataFrame, counterparties_clean: DataFrame,
                   prices_clean: DataFrame):
    ins = instruments_clean.select("instrument_id", F.col("currency").alias("_ins_ccy"),
                                   F.lit(True).alias("_ins_ok"))
    cp = counterparties_clean.select("counterparty_id", F.lit(True).alias("_cp_ok"))
    px = prices_clean.select("instrument_id", F.col("price_date").alias("trade_date"),
                             F.col("close_price").alias("_close"))
    df = (df.withColumn("_id_rank", F.row_number().over(Window.partitionBy("trade_id").orderBy("ingest_seq")))
            .join(F.broadcast(ins), "instrument_id", "left")
            .join(F.broadcast(cp), "counterparty_id", "left")
            .join(px, ["instrument_id", "trade_date"], "left"))
    return df, ["_id_rank", "_ins_ccy", "_ins_ok", "_cp_ok", "_close"]


def trade_rules(cfg: Config) -> list[Rule]:
    d = "trades"
    return [
        Rule("TRD_001", d, "Trade ID is populated", COMPLETENESS, CRITICAL,
             lambda: _blank("trade_id"), "Primary key for reconciliation."),
        Rule("TRD_002", d, "Trade ID is unique (first delivery wins)", UNIQUENESS, CRITICAL,
             lambda: F.col("_id_rank") > 1, "Re-delivered trades would be booked twice."),
        Rule("TRD_003", d, "Side is BUY or SELL", VALIDITY, CRITICAL,
             lambda: ~F.col("side").isin(*TRADE_SIDES), "Direction sets the sign of position and exposure."),
        Rule("TRD_004", d, "Quantity is positive", VALIDITY, CRITICAL,
             lambda: F.col("quantity").isNull() | (F.col("quantity") <= 0), "Direction is carried by side, not sign."),
        Rule("TRD_005", d, "Settlement date is on or after trade date", CONSISTENCY, CRITICAL,
             lambda: F.col("settle_date") < F.col("trade_date"), "Time-travelling settlement."),
        Rule("TRD_006", d, "Instrument exists in the validated instrument master", INTEGRITY, CRITICAL,
             lambda: F.col("_ins_ok").isNull(), "Cannot value a trade on an unknown / quarantined security."),
        Rule("TRD_007", d, "Counterparty exists in the validated counterparty master", INTEGRITY, CRITICAL,
             lambda: F.col("_cp_ok").isNull(), "Cannot attach exposure to an unknown legal entity."),
        Rule("TRD_008", d, "Trade date is not in the future", VALIDITY, CRITICAL,
             lambda: F.col("trade_date") > F.lit(cfg.as_of), "Future-dated trade in the blotter."),
        Rule("TRD_009", d, f"Price within {int(cfg.trade_price_tolerance * 100)}% of trade-date close", ACCURACY,
             CRITICAL, lambda: F.abs(F.col("price") / F.col("_close") - 1) > cfg.trade_price_tolerance,
             "Fat-finger / mis-priced trade would distort MtM and exposure."),
        Rule("TRD_010", d, "Trade currency matches instrument currency", CONSISTENCY, CRITICAL,
             lambda: F.col("currency") != F.col("_ins_ccy"), "Currency mismatch breaks FX conversion."),
        Rule("TRD_011", d, "Status is a recognised lifecycle value", VALIDITY, CRITICAL,
             lambda: ~F.col("status").isin(*TRADE_STATUSES), "Open / closed logic keys on status."),
    ]


# ------------------------------------------------------------------------------------------------
# Settlements (back office)
# ------------------------------------------------------------------------------------------------
def prepare_settlements(df: DataFrame):
    df = df.withColumn("_id_rank", F.row_number().over(Window.partitionBy("trade_id").orderBy("ingest_seq")))
    return df, ["_id_rank"]


def settlement_rules(cfg: Config) -> list[Rule]:
    d = "settlements"
    return [
        Rule("SET_001", d, "Trade ID is populated", COMPLETENESS, CRITICAL, lambda: _blank("trade_id"),
             "Join key for reconciliation."),
        Rule("SET_002", d, "One settlement record per trade", UNIQUENESS, CRITICAL, lambda: F.col("_id_rank") > 1,
             "Duplicate instructions risk double settlement."),
        Rule("SET_003", d, "Settled quantity is positive", VALIDITY, CRITICAL,
             lambda: F.col("quantity").isNull() | (F.col("quantity") <= 0), "Empty settlement instruction."),
    ]


# ------------------------------------------------------------------------------------------------
# Collateral
# ------------------------------------------------------------------------------------------------
def prepare_collateral(df: DataFrame, counterparties_clean: DataFrame):
    cp = counterparties_clean.select("counterparty_id", F.lit(True).alias("_cp_ok"))
    return df.join(F.broadcast(cp), "counterparty_id", "left"), ["_cp_ok"]


def collateral_rules(cfg: Config) -> list[Rule]:
    d = "collateral"
    return [
        Rule("COL_001", d, "Counterparty exists in the validated master", INTEGRITY, CRITICAL,
             lambda: F.col("_cp_ok").isNull(), "Collateral must belong to a known legal entity."),
        Rule("COL_002", d, "Collateral balance is non-negative", VALIDITY, CRITICAL,
             lambda: F.col("collateral_held_usd").isNull() | (F.col("collateral_held_usd") < 0), "Negative balance."),
        Rule("COL_003", d, "Collateral balance is as of the run date", TIMELINESS, WARNING,
             lambda: F.col("collateral_as_of") != F.lit(cfg.as_of),
             "A stale balance can trigger a wrong margin call."),
    ]


# Dataset-level rule that cannot be expressed as a row predicate (listed in the catalogue for completeness)
PRC_CALENDAR_RULE = Rule("PRC_006", "prices", "Price exists for every instrument on every business day",
                         COMPLETENESS, WARNING, lambda: F.lit(False),
                         "Gaps force multi-day returns and understate daily volatility.")


def catalogue(cfg: Config | None = None) -> list[Rule]:
    cfg = cfg or Config()
    return (instrument_rules(cfg) + counterparty_rules(cfg) + price_rules(cfg) + [PRC_CALENDAR_RULE]
            + fx_rules(cfg) + trade_rules(cfg) + settlement_rules(cfg) + collateral_rules(cfg))
