from datetime import date, timedelta

from pyspark.sql import functions as F

from pmdp import dq_rules as R
from pmdp.config import Config
from pmdp.data_quality import apply_rules
from pmdp.ingest import SCHEMAS

CFG = Config()


def frame(spark, name, rows):
    return spark.createDataFrame(rows, SCHEMAS[name]).withColumn("ingest_seq", F.monotonically_increasing_id())


def failed_by_id(outcome, key):
    return {r[key]: set(r["dq_failed_rules"].split("|")) for r in outcome.quarantine.collect()}


def test_instrument_rules(spark):
    future = CFG.as_of + timedelta(days=1000)
    rows = [  # id, isin, ticker, name, class, ccy, country, sector, mult, maturity
        ("OK", "US0378331005", "A", "ok", "EQUITY", "USD", "US", "Tech", 1.0, None),
        ("BADISIN", "US0378331006", "B", "x", "EQUITY", "USD", "US", "Tech", 1.0, None),
        ("DUPISIN", "US0378331005", "C", "x", "EQUITY", "USD", "US", "Tech", 1.0, None),
        ("NOCCY", "GB0002634946", "D", "x", "EQUITY", None, "GB", "Tech", 1.0, None),
        ("OLDBOND", "DE0001102333", "E", "x", "BOND", "EUR", "DE", "Govt", 0.01, CFG.as_of - timedelta(days=1)),
        ("NOSECTOR", "FR0000120271", "F", "x", "EQUITY", "EUR", "FR", None, 1.0, None),
        ("GOODBOND", "DE0001102341", "G", "x", "BOND", "EUR", "DE", "Govt", 0.01, future),
    ]
    prepared, helpers = R.prepare_instruments(frame(spark, "instruments", rows))
    out = apply_rules(prepared, "instruments", R.instrument_rules(CFG), helpers)

    failed = failed_by_id(out, "instrument_id")
    assert "INS_003" in failed["BADISIN"]
    assert failed["DUPISIN"] == {"INS_004"}
    assert failed["NOCCY"] == {"INS_005"}
    assert failed["OLDBOND"] == {"INS_009"}
    # WARNING-only rows are reported but stay in the clean set
    clean_ids = {r["instrument_id"] for r in out.clean.collect()}
    assert "NOSECTOR" in clean_ids and "OK" in clean_ids
    sector = next(r for r in out.results if r["rule_id"] == "INS_008")
    assert sector["failed_rows"] == 1 and sector["status"] == "WARN"


def test_trade_rules_catch_each_fault(spark):
    d = date(2026, 11, 2)
    ins = spark.createDataFrame([("INS1", "USD")], ["instrument_id", "currency"])
    cpt = spark.createDataFrame([("CP1",)], ["counterparty_id"])
    px = spark.createDataFrame([("INS1", d, 100.0)], ["instrument_id", "price_date", "close_price"])
    base = dict(trade_date=d, settle_date=d + timedelta(days=2), instrument_id="INS1", counterparty_id="CP1",
                side="BUY", quantity=100.0, price=100.5, currency="USD", status="CONFIRMED")

    def trade(tid, **over):
        t = {**base, **over}
        return (tid, t["trade_date"], t["settle_date"], t["instrument_id"], t["counterparty_id"], t["side"],
                t["quantity"], t["price"], t["currency"], t["status"])

    rows = [trade("T1"), trade("T1"), trade("T3", quantity=0.0), trade("T4", settle_date=d - timedelta(days=1)),
            trade("T5", instrument_id="NOPE"), trade("T6", counterparty_id="NOPE"), trade("T7", price=120.0),
            trade("T8", side="HOLD"), trade("T9", currency="EUR")]
    prepared, helpers = R.prepare_trades(frame(spark, "trades", rows), ins, cpt, px)
    out = apply_rules(prepared, "trades", R.trade_rules(CFG), helpers)

    failed = failed_by_id(out, "trade_id")
    assert failed["T1"] == {"TRD_002"}                      # only the re-delivery is rejected
    assert failed["T3"] == {"TRD_004"}
    assert failed["T4"] == {"TRD_005"}
    assert failed["T5"] == {"TRD_006"}
    assert failed["T6"] == {"TRD_007"}
    assert failed["T7"] == {"TRD_009"}
    assert failed["T8"] == {"TRD_003"}
    assert failed["T9"] == {"TRD_010"}
    assert [r["trade_id"] for r in out.clean.collect()] == ["T1"]
    assert out.total_rows == 9 and out.quarantined_rows == 8


def test_price_rules_spike_stale_negative_duplicate(spark):
    d0 = date(2026, 10, 1)
    seq = [100, 101, 102, 500, 103, 104, 104, 104, 104, 104, 105]     # spike at idx 3, 5 identical closes idx 5-9
    rows = [(d0 + timedelta(days=i), "INS1", float(p), "USD") for i, p in enumerate(seq)]
    rows += [(d0 + timedelta(days=i), "INS2", float(p), "USD") for i, p in enumerate([50, -5, 51])]
    rows += [(d0, "INS1", 100.0, "USD")]                                # re-delivered duplicate
    ids = spark.createDataFrame([("INS1",), ("INS2",)], ["instrument_id"])
    prepared, helpers = R.prepare_prices(frame(spark, "prices", rows), CFG, ids)
    out = apply_rules(prepared, "prices", R.price_rules(CFG), helpers)

    by = {(r["instrument_id"], r["price_date"]): set(r["dq_failed_rules"].split("|")) for r in out.quarantine.collect()}
    assert by[("INS1", d0 + timedelta(days=3))] == {"PRC_004"}
    assert by[("INS2", d0 + timedelta(days=1))] == {"PRC_001"}
    assert any(k[0] == "INS1" and k[1] == d0 and v == {"PRC_003"} for k, v in by.items())
    stale = next(r for r in out.results if r["rule_id"] == "PRC_005")
    assert stale["failed_rows"] == 1 and stale["status"] == "WARN"     # warning only - row stays in clean set
