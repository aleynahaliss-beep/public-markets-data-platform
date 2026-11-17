"""Market risk, counterparty credit risk and margining.

Everything here is an *illustrative* implementation of well-known mechanics (historical-simulation
VaR, netting-set exposure with a CEM-style add-on, CSA variation margin). It is not a regulatory
engine (no SA-CCR / IMM / FRTB) - see docs/01_business_requirements.md, section 'Out of scope'.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

from .config import ADDON_FACTORS, NETTING_FLOOR, NETTING_WEIGHT, OPEN_STATUSES, Config


# ------------------------------------------------------------------------------------------------
# Prices, positions
# ------------------------------------------------------------------------------------------------
def build_prices_usd(prices_clean: DataFrame, instruments_clean: DataFrame, fx_clean: DataFrame,
                     cfg: Config) -> DataFrame:
    ins = instruments_clean.select("instrument_id", "asset_class", "currency", "price_multiplier")
    fx = fx_clean.select(F.col("rate_date").alias("price_date"), "currency", "usd_per_unit")
    return (prices_clean.select("instrument_id", "price_date", "close_price")
            .filter(F.col("price_date") <= F.lit(cfg.as_of))
            .join(ins, "instrument_id").join(fx, ["price_date", "currency"])
            .withColumn("price_usd", F.col("close_price") * F.col("usd_per_unit"))
            .select("instrument_id", "asset_class", "currency", "price_multiplier", "price_date",
                    "close_price", "price_usd"))


def latest_prices(prices_usd: DataFrame) -> DataFrame:
    w = Window.partitionBy("instrument_id").orderBy(F.col("price_date").desc())
    return (prices_usd.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1)
            .select("instrument_id", F.col("price_date").alias("latest_price_date"),
                    F.col("price_usd").alias("latest_price_usd")))


def compute_positions(trades_clean: DataFrame, instruments_clean: DataFrame, latest_px: DataFrame) -> DataFrame:
    signed = F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(-F.col("quantity"))
    pos = (trades_clean.filter(F.col("status") != "CANCELLED").groupBy("instrument_id")
           .agg(F.sum(signed).alias("net_quantity"), F.count(F.lit(1)).alias("trade_count")))
    ins = instruments_clean.select("instrument_id", "asset_class", "price_multiplier")
    return (pos.join(ins, "instrument_id").join(latest_px, "instrument_id", "left")
            .withColumn("market_value_usd",
                        F.round(F.col("net_quantity") * F.col("price_multiplier") * F.col("latest_price_usd"), 2))
            .select("instrument_id", "asset_class", "net_quantity", "trade_count", "latest_price_date",
                    F.round("latest_price_usd", 6).alias("latest_price_usd"), "market_value_usd"))


# ------------------------------------------------------------------------------------------------
# Market risk: historical-simulation VaR / ES
# ------------------------------------------------------------------------------------------------
def daily_returns(prices_usd: DataFrame) -> DataFrame:
    w = Window.partitionBy("instrument_id").orderBy("price_date")
    return (prices_usd.withColumn("_prev", F.lag("price_usd").over(w))
            .withColumn("ret", F.when(F.col("_prev") > 0, F.col("price_usd") / F.col("_prev") - 1))
            .filter(F.col("ret").isNotNull()).select("instrument_id", "asset_class", "price_date", "ret"))


def compute_var(positions: DataFrame, returns: DataFrame, cfg: Config) -> DataFrame:
    """1-day historical-simulation VaR and Expected Shortfall over the last `var_window` scenarios.

    Scenario P&L(d) = sum_i MV_i * r_i(d), i.e. today's positions revalued with each historical
    day's return (full revaluation is linear for these cash products).
    """
    dates = returns.select("price_date").distinct().orderBy(F.col("price_date").desc()).limit(cfg.var_window)
    pnl = (returns.join(F.broadcast(dates), "price_date")
           .join(positions.select("instrument_id", "market_value_usd"), "instrument_id")
           .withColumn("pnl", F.col("market_value_usd") * F.col("ret")))
    by_class = pnl.groupBy("asset_class", "price_date").agg(F.sum("pnl").alias("pnl")) \
        .withColumnRenamed("asset_class", "scope")
    total = pnl.groupBy("price_date").agg(F.sum("pnl").alias("pnl")).withColumn("scope", F.lit("PORTFOLIO"))
    scen = total.select("scope", "price_date", "pnl").unionByName(by_class.select("scope", "price_date", "pnl")).cache()

    q = round(1.0 - cfg.var_confidence, 6)
    quant = scen.groupBy("scope").agg(F.expr(f"percentile(pnl, {q})").alias("_q"),
                                      F.count(F.lit(1)).alias("n_scenarios"), F.min("pnl").alias("worst_pnl"))
    es = (scen.join(quant, "scope").filter(F.col("pnl") <= F.col("_q")).groupBy("scope")
          .agg(F.avg("pnl").alias("_es")))
    return (quant.join(es, "scope", "left")
            .select("scope", "n_scenarios", F.round(-F.col("_q"), 2).alias("var_usd"),
                    F.round(-F.col("_es"), 2).alias("es_usd"), F.round(F.col("worst_pnl"), 2).alias("worst_pnl_usd"))
            .orderBy(F.when(F.col("scope") == "PORTFOLIO", 0).otherwise(1), "scope"))


# ------------------------------------------------------------------------------------------------
# Counterparty credit risk
# ------------------------------------------------------------------------------------------------
def _addon_factor() -> "F.Column":
    expr = F.lit(0.0)
    for cls, f in ADDON_FACTORS.items():
        expr = F.when(F.col("asset_class") == cls, F.lit(f)).otherwise(expr)
    return expr


def compute_counterparty_exposure(trades_clean: DataFrame, instruments_clean: DataFrame,
                                  counterparties_clean: DataFrame, latest_px: DataFrame,
                                  fx_clean: DataFrame, cfg: Config) -> DataFrame:
    """Netting-set exposure on open (unsettled, non-cancelled) trades.

    MtM_i        = sign * qty * multiplier * (latest_px_usd - trade_px_usd)      (+ = counterparty owes us)
    Replacement  = max(sum MtM, 0)
    PFE add-on   = sum(notional * factor * sqrt(min(T, 1))) * (0.4 + 0.6 * NGR)   (illustrative, CEM-style)
    EAD estimate = Replacement + PFE
    """
    open_t = trades_clean.filter(F.col("status").isin(*OPEN_STATUSES) & (F.col("settle_date") > F.lit(cfg.as_of)))
    ins = instruments_clean.select("instrument_id", "asset_class", "price_multiplier")
    fx = fx_clean.select(F.col("rate_date").alias("trade_date"), "currency", F.col("usd_per_unit").alias("_fx_trade"))
    t = (open_t.join(ins, "instrument_id").join(latest_px, "instrument_id").join(fx, ["trade_date", "currency"]))

    sign = F.when(F.col("side") == "BUY", F.lit(1.0)).otherwise(F.lit(-1.0))
    years = F.greatest(F.datediff(F.col("settle_date"), F.lit(cfg.as_of)) / F.lit(365.0), F.lit(1 / 365.0))
    t = (t.withColumn("mtm_usd", sign * F.col("quantity") * F.col("price_multiplier")
                      * (F.col("latest_price_usd") - F.col("price") * F.col("_fx_trade")))
          .withColumn("notional_usd", F.col("quantity") * F.col("price_multiplier") * F.col("latest_price_usd"))
          .withColumn("addon_usd", F.col("notional_usd") * _addon_factor() * F.sqrt(F.least(years, F.lit(1.0)))))

    agg = (t.groupBy("counterparty_id")
           .agg(F.count(F.lit(1)).alias("open_trades"), F.sum("notional_usd").alias("gross_notional_usd"),
                F.sum(F.greatest(F.col("mtm_usd"), F.lit(0.0))).alias("gross_positive_mtm_usd"),
                F.sum("mtm_usd").alias("net_mtm_usd"), F.sum("addon_usd").alias("gross_addon_usd")))
    rc = F.greatest(F.col("net_mtm_usd"), F.lit(0.0))
    ngr = F.when(F.col("gross_positive_mtm_usd") > 0, rc / F.col("gross_positive_mtm_usd")).otherwise(F.lit(0.0))
    pfe = F.col("gross_addon_usd") * (F.lit(NETTING_FLOOR) + F.lit(NETTING_WEIGHT) * ngr)

    cp = counterparties_clean.select("counterparty_id", "counterparty_name", "rating", "csa_flag")
    out = (agg.join(F.broadcast(cp), "counterparty_id", "left")
           .withColumn("replacement_cost_usd", rc).withColumn("pfe_addon_usd", pfe)
           .withColumn("ead_estimate_usd", rc + pfe))
    money = ["gross_notional_usd", "gross_positive_mtm_usd", "net_mtm_usd", "replacement_cost_usd",
             "pfe_addon_usd", "ead_estimate_usd"]
    out = out.select("counterparty_id", "counterparty_name", "rating", "csa_flag", "open_trades",
                     *[F.round(c, 2).alias(c) for c in money])
    return out.orderBy(F.desc("ead_estimate_usd"))


# ------------------------------------------------------------------------------------------------
# Margining / collateral
# ------------------------------------------------------------------------------------------------
def compute_margin(exposure: DataFrame, counterparties_clean: DataFrame, collateral_clean: DataFrame) -> DataFrame:
    """Variation-margin call / return per CSA counterparty.

    required   = max(net_MtM - threshold, 0)          collateral we should be holding
    raw_call   = required - held                      (+ call, - return)
    amount     = 0 if |raw_call| < MTA, else rounded to the CSA rounding increment
                 (calls round up, returns round down)
    """
    cp = counterparties_clean.select("counterparty_id", "counterparty_name", "rating", "csa_flag",
                                     "threshold_usd", "mta_usd", "rounding_usd")
    exp = exposure.select("counterparty_id", "net_mtm_usd")
    col = collateral_clean.select("counterparty_id", "collateral_held_usd")
    df = (cp.join(exp, "counterparty_id", "left").join(col, "counterparty_id", "left")
          .withColumn("net_mtm_usd", F.coalesce("net_mtm_usd", F.lit(0.0)))
          .withColumn("collateral_held_usd", F.coalesce("collateral_held_usd", F.lit(0.0))))

    is_csa = F.col("csa_flag") == "Y"
    required = F.when(is_csa, F.greatest(F.col("net_mtm_usd") - F.col("threshold_usd"), F.lit(0.0))).otherwise(F.lit(0.0))
    df = df.withColumn("required_collateral_usd", required)
    raw = F.col("required_collateral_usd") - F.col("collateral_held_usd")
    mag = F.abs(raw)
    rounded = F.when(raw > 0, F.ceil(mag / F.col("rounding_usd")) * F.col("rounding_usd")) \
               .otherwise(F.floor(mag / F.col("rounding_usd")) * F.col("rounding_usd"))
    amount = F.when(is_csa & (mag >= F.col("mta_usd")), F.signum(raw) * rounded).otherwise(F.lit(0.0))
    df = df.withColumn("margin_amount_usd", amount)
    action = (F.when(~is_csa, "NO_CSA").when(F.col("margin_amount_usd") > 0, "MARGIN_CALL")
               .when(F.col("margin_amount_usd") < 0, "COLLATERAL_RETURN").otherwise("NO_ACTION"))
    uncoll = F.when(is_csa, F.greatest(F.col("net_mtm_usd") - F.col("collateral_held_usd"), F.lit(0.0))) \
              .otherwise(F.greatest(F.col("net_mtm_usd"), F.lit(0.0)))
    money = ["net_mtm_usd", "collateral_held_usd", "required_collateral_usd", "margin_amount_usd"]
    return (df.withColumn("margin_action", action).withColumn("uncollateralised_exposure_usd", uncoll)
            .select("counterparty_id", "counterparty_name", "rating", "csa_flag", "threshold_usd", "mta_usd",
                    *[F.round(c, 2).alias(c) for c in money], "margin_action",
                    F.round("uncollateralised_exposure_usd", 2).alias("uncollateralised_exposure_usd"))
            .orderBy("counterparty_id"))
