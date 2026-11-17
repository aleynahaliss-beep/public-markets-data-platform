from datetime import date, timedelta

import pytest

from pmdp.config import Config
from pmdp.risk import compute_counterparty_exposure, compute_margin, compute_positions, compute_var

CFG = Config()


def test_historical_var_and_es_are_exact(spark):
    """101 scenarios with returns -5.0% ... +5.0% on a USD 1m position -> 1% quantile is -4.9%."""
    d0 = date(2025, 1, 1)
    returns = spark.createDataFrame(
        [("INS1", "EQUITY", d0 + timedelta(days=k), (k - 50) / 1000.0) for k in range(101)],
        ["instrument_id", "asset_class", "price_date", "ret"])
    positions = spark.createDataFrame([("INS1", 1_000_000.0)], ["instrument_id", "market_value_usd"])
    out = {r["scope"]: r for r in compute_var(positions, returns, CFG).collect()}
    assert out["PORTFOLIO"]["n_scenarios"] == 101
    assert out["PORTFOLIO"]["var_usd"] == pytest.approx(49_000.0)
    assert out["PORTFOLIO"]["es_usd"] == pytest.approx(49_500.0)        # mean of the two worst days
    assert out["PORTFOLIO"]["worst_pnl_usd"] == pytest.approx(-50_000.0)
    assert out["EQUITY"]["var_usd"] == out["PORTFOLIO"]["var_usd"]


def test_short_position_has_losses_on_up_moves(spark):
    d0 = date(2025, 1, 1)
    returns = spark.createDataFrame(
        [("INS1", "EQUITY", d0 + timedelta(days=k), (k - 50) / 1000.0) for k in range(101)],
        ["instrument_id", "asset_class", "price_date", "ret"])
    positions = spark.createDataFrame([("INS1", -1_000_000.0)], ["instrument_id", "market_value_usd"])
    out = {r["scope"]: r for r in compute_var(positions, returns, CFG).collect()}
    assert out["PORTFOLIO"]["var_usd"] == pytest.approx(49_000.0)


def test_exposure_netting_and_addon(spark):
    as_of = CFG.as_of
    far = as_of + timedelta(days=365)             # residual maturity of exactly 1 year -> sqrt(1) = 1
    d = date(2026, 10, 1)
    cols = ["trade_id", "trade_date", "settle_date", "instrument_id", "counterparty_id", "side", "quantity",
            "price", "currency", "status"]
    trades = spark.createDataFrame([
        ("A", d, far, "INS1", "CP1", "BUY", 1000.0, 100.0, "USD", "CONFIRMED"),    # MtM +10,000
        ("B", d, far, "INS1", "CP1", "SELL", 500.0, 105.0, "USD", "CONFIRMED"),    # MtM  -2,500
        ("C", d, as_of - timedelta(days=1), "INS1", "CP1", "BUY", 9999.0, 50.0, "USD", "SETTLED"),   # ignored
        ("D", d, far, "INS1", "CP1", "BUY", 9999.0, 50.0, "USD", "CANCELLED"),                      # ignored
    ], cols)
    ins = spark.createDataFrame([("INS1", "EQUITY", 1.0)], ["instrument_id", "asset_class", "price_multiplier"])
    cpt = spark.createDataFrame([("CP1", "Cpty", "A", "Y")], ["counterparty_id", "counterparty_name", "rating", "csa_flag"])
    latest = spark.createDataFrame([("INS1", as_of, 110.0)], ["instrument_id", "latest_price_date", "latest_price_usd"])
    fx = spark.createDataFrame([(d, "USD", 1.0)], ["rate_date", "currency", "usd_per_unit"])

    row = compute_counterparty_exposure(trades, ins, cpt, latest, fx, CFG).collect()[0]
    assert row["open_trades"] == 2
    assert row["net_mtm_usd"] == pytest.approx(7_500.0)
    assert row["gross_positive_mtm_usd"] == pytest.approx(10_000.0)
    assert row["replacement_cost_usd"] == pytest.approx(7_500.0)
    # notional 165,000 * 20% add-on = 33,000; netting ratio 7,500/10,000 = 0.75 -> * (0.4 + 0.6*0.75) = 0.85
    assert row["pfe_addon_usd"] == pytest.approx(28_050.0)
    assert row["ead_estimate_usd"] == pytest.approx(35_550.0)


def test_margin_call_rounding_mta_and_returns(spark):
    cp_cols = ["counterparty_id", "counterparty_name", "rating", "csa_flag", "threshold_usd", "mta_usd", "rounding_usd"]
    cpts = spark.createDataFrame([
        ("A", "A", "A", "Y", 100_000.0, 25_000.0, 5_000.0),
        ("B", "B", "A", "Y", 100_000.0, 25_000.0, 5_000.0),
        ("C", "C", "A", "Y", 100_000.0, 25_000.0, 5_000.0),
        ("D", "D", "A", "N", None, None, None),
        ("E", "E", "A", "Y", 100_000.0, 25_000.0, 5_000.0),
        ("F", "F", "A", "Y", 100_000.0, 25_000.0, 5_000.0),     # no open trades at all
    ], cp_cols)
    exposure = spark.createDataFrame([("A", 389_290.18), ("B", 147_520.66), ("C", 340.39), ("D", 106_382.29),
                                      ("E", 133_333.33)], ["counterparty_id", "net_mtm_usd"])
    collateral = spark.createDataFrame([("A", 70_000.0), ("B", 35_000.0), ("C", 105_000.0), ("E", 0.0), ("F", 12_000.0)],
                                       ["counterparty_id", "collateral_held_usd"])
    out = {r["counterparty_id"]: r for r in compute_margin(exposure, cpts, collateral).collect()}

    assert out["A"]["margin_action"] == "MARGIN_CALL"
    assert out["A"]["margin_amount_usd"] == 220_000.0               # 219,290.18 rounded UP to the 5k increment
    assert out["B"]["margin_action"] == "NO_ACTION"                 # 12,520.66 shortfall is below the 25k MTA
    assert out["B"]["margin_amount_usd"] == 0.0
    assert out["C"]["margin_action"] == "COLLATERAL_RETURN"
    assert out["C"]["margin_amount_usd"] == -105_000.0              # full excess returned
    assert out["D"]["margin_action"] == "NO_CSA"
    assert out["D"]["uncollateralised_exposure_usd"] == pytest.approx(106_382.29)
    assert out["E"]["margin_amount_usd"] == 35_000.0                # 33,333.33 -> 35,000
    assert out["F"]["margin_action"] == "NO_ACTION"                 # 12k excess is below the 25k MTA: keep it
    assert out["F"]["margin_amount_usd"] == 0.0


def test_positions_exclude_cancelled_and_sign_sells(spark):
    cols = ["trade_id", "instrument_id", "side", "quantity", "status"]
    trades = spark.createDataFrame([
        ("1", "INS1", "BUY", 1000.0, "SETTLED"),
        ("2", "INS1", "SELL", 300.0, "CONFIRMED"),
        ("3", "INS1", "BUY", 5000.0, "CANCELLED"),      # must not count
        ("4", "INS2", "SELL", 100000.0, "SETTLED"),     # bond, face amount, short
    ], cols)
    ins = spark.createDataFrame([("INS1", "EQUITY", 1.0), ("INS2", "BOND", 0.01)],
                                ["instrument_id", "asset_class", "price_multiplier"])
    latest = spark.createDataFrame([("INS1", CFG.as_of, 50.0), ("INS2", CFG.as_of, 101.0)],
                                   ["instrument_id", "latest_price_date", "latest_price_usd"])
    pos = {r["instrument_id"]: r for r in compute_positions(trades, ins, latest).collect()}
    assert pos["INS1"]["net_quantity"] == 700.0 and pos["INS1"]["trade_count"] == 2
    assert pos["INS1"]["market_value_usd"] == pytest.approx(35_000.0)
    assert pos["INS2"]["net_quantity"] == -100_000.0
    assert pos["INS2"]["market_value_usd"] == pytest.approx(-101_000.0)     # 100,000 face * 0.01 * 101
