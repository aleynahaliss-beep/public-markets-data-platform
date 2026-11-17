from datetime import date

import pytest

from pmdp.config import Config
from pmdp.reconciliation import reconcile

D = date(2026, 11, 12)
D2 = date(2026, 11, 13)


def test_every_break_category(spark):
    trades = spark.createDataFrame([
        ("T1", D, "CP1", "INS1", 100.0, 10.0, "CONFIRMED"),   # matches
        ("T2", D, "CP1", "INS1", 100.0, 10.0, "CONFIRMED"),   # BO quantity differs
        ("T3", D, "CP2", "INS1", 100.0, 10.0, "CONFIRMED"),   # BO amount differs
        ("T4", D, "CP2", "INS1", 100.0, 10.0, "CONFIRMED"),   # BO settle date differs
        ("T5", D, "CP2", "INS1", 100.0, 10.0, "CONFIRMED"),   # no BO record
        ("T6", D, "CP2", "INS1", 100.0, 10.0, "CANCELLED"),   # cancelled -> not expected in BO
    ], ["trade_id", "settle_date", "counterparty_id", "instrument_id", "quantity", "price", "status"])
    ins = spark.createDataFrame([("INS1", 1.0)], ["instrument_id", "price_multiplier"])
    bo = spark.createDataFrame([
        ("T1", D, 100.0, 1000.0),
        ("T2", D, 110.0, 1100.0),
        ("T3", D, 100.0, 1003.0),
        ("T4", D2, 100.0, 1000.0),
        ("T7", D, 1.0, 5.0),            # BO-only, unknown to the front office
        ("T8", D, 1.0, 5.0),            # BO-only, but front office row was rejected by DQ
    ], ["trade_id", "settle_date", "quantity", "net_amount"])
    rejected = spark.createDataFrame([("T8",)], ["trade_id"])

    out = reconcile(trades, ins, bo, rejected, Config())
    cat = {r["trade_id"]: r["break_category"] for r in out.collect()}
    assert cat == {"T1": "MATCHED", "T2": "QUANTITY_BREAK", "T3": "AMOUNT_BREAK", "T4": "SETTLE_DATE_BREAK",
                   "T5": "MISSING_IN_BACK_OFFICE", "T7": "MISSING_IN_FRONT_OFFICE", "T8": "FO_REJECTED_BY_DQ"}
    t3 = next(r for r in out.collect() if r["trade_id"] == "T3")
    assert t3["amount_diff"] == pytest.approx(-3.0)


def test_amount_within_tolerance_is_a_match(spark):
    trades = spark.createDataFrame([("T1", D, "CP1", "INS1", 100.0, 10.0, "SETTLED")],
                                   ["trade_id", "settle_date", "counterparty_id", "instrument_id", "quantity", "price", "status"])
    ins = spark.createDataFrame([("INS1", 1.0)], ["instrument_id", "price_multiplier"])
    bo = spark.createDataFrame([("T1", D, 100.0, 1000.30)], ["trade_id", "settle_date", "quantity", "net_amount"])
    empty = spark.createDataFrame([], "trade_id string")
    out = reconcile(trades, ins, bo, empty, Config(recon_amount_tolerance=0.50))
    assert out.collect()[0]["break_category"] == "MATCHED"
