"""End-to-end orchestration: ingest -> DQ gate -> reconcile -> risk -> SQL reports.

    python -m pmdp.pipeline            # generate sample feeds if missing, run everything
    python -m pmdp.pipeline --regen    # force fresh synthetic feeds
"""

from __future__ import annotations

import argparse
import shutil
import time
from dataclasses import replace
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from . import dq_rules as R
from .calendar_utils import business_days
from .config import Config
from .data_quality import DQOutcome, apply_rules, result_record
from .generate_data import generate
from .ingest import read_all
from .io_utils import write_df_csv, write_rows
from .reconciliation import recon_summary, reconcile
from .risk import (build_prices_usd, compute_counterparty_exposure, compute_margin, compute_positions,
                   compute_var, daily_returns, latest_prices)
from .spark_session import get_spark
from .sql_reports import run_sql_reports

DQ_FIELDS = ["dataset", "rule_id", "rule_name", "dimension", "severity", "total_rows", "failed_rows",
             "fail_rate", "max_fail_rate", "status"]


def price_calendar_check(spark: SparkSession, prices_raw: DataFrame, instruments_raw: DataFrame, cfg: Config):
    """PRC_006: every instrument must have a price on every business day of the history window."""
    cal = spark.createDataFrame([(d,) for d in business_days(cfg.as_of, cfg.history_days)], ["price_date"])
    ids = instruments_raw.select("instrument_id").where(F.col("instrument_id").isNotNull()).distinct()
    expected = ids.crossJoin(cal)
    present = prices_raw.select("instrument_id", "price_date").distinct()
    missing = expected.join(present, ["instrument_id", "price_date"], "left_anti")
    return result_record(R.PRC_CALENDAR_RULE, expected.count(), missing.count()), missing.orderBy("instrument_id", "price_date")


def run(cfg: Config, regen: bool = False, show: int = 0) -> dict:
    t0 = time.time()
    raw_dir, out_dir = Path(cfg.raw_dir), Path(cfg.out_dir)
    if regen or not (raw_dir / "trades.csv").exists():
        generate(cfg, raw_dir)
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    spark = get_spark()
    raw = read_all(spark, raw_dir)
    results: list[dict] = []
    summary: list[dict] = []

    def gate(name: str, prepared: DataFrame, rules) -> DQOutcome:
        """Apply a dataset's rules, record results and persist its quarantine table."""
        out = apply_rules(prepared, name, rules, helpers[name])
        results.extend(out.results)
        summary.append({"dataset": name, "rows_in": out.total_rows, "rows_clean": out.total_rows - out.quarantined_rows,
                        "rows_quarantined": out.quarantined_rows})
        write_df_csv(out.quarantine.orderBy("ingest_seq"), out_dir / "quarantine" / f"{name}.csv")
        return out

    helpers: dict[str, list[str]] = {}

    # ---- 1. reference data ---------------------------------------------------------------------
    p, helpers["instruments"] = R.prepare_instruments(raw["instruments"])
    ins = gate("instruments", p, R.instrument_rules(cfg))
    p, helpers["counterparties"] = R.prepare_counterparties(raw["counterparties"])
    cpt = gate("counterparties", p, R.counterparty_rules(cfg))

    # ---- 2. market data ------------------------------------------------------------------------
    p, helpers["prices"] = R.prepare_prices(raw["prices"], cfg, raw["instruments"])
    prc = gate("prices", p, R.price_rules(cfg))
    cal_result, missing_prices = price_calendar_check(spark, raw["prices"], raw["instruments"], cfg)
    results.append(cal_result)
    write_df_csv(missing_prices, out_dir / "quarantine" / "prices_missing_dates.csv")
    p, helpers["fx_rates"] = R.prepare_fx(raw["fx_rates"])
    fx = gate("fx_rates", p, R.fx_rules(cfg))

    # ---- 3. transactions -----------------------------------------------------------------------
    p, helpers["trades"] = R.prepare_trades(raw["trades"], ins.clean, cpt.clean, prc.clean)
    trd = gate("trades", p, R.trade_rules(cfg))
    p, helpers["settlements"] = R.prepare_settlements(raw["settlements"])
    stl = gate("settlements", p, R.settlement_rules(cfg))
    p, helpers["collateral"] = R.prepare_collateral(raw["collateral"], cpt.clean)
    col = gate("collateral", p, R.collateral_rules(cfg))

    write_rows(out_dir / "dq_results.csv", DQ_FIELDS, results)
    write_rows(out_dir / "dq_dataset_summary.csv", ["dataset", "rows_in", "rows_clean", "rows_quarantined"], summary)

    # ---- 4. reconciliation ---------------------------------------------------------------------
    recon = reconcile(trd.clean, ins.clean, stl.clean, trd.quarantine, cfg).cache()
    write_df_csv(recon.orderBy("recon_status", "break_category", "trade_id"), out_dir / "recon_trades.csv")
    summ = recon_summary(recon)
    write_df_csv(summ, out_dir / "recon_summary.csv")

    # ---- 5. risk -------------------------------------------------------------------------------
    prices_usd = build_prices_usd(prc.clean, ins.clean, fx.clean, cfg).cache()
    latest = latest_prices(prices_usd)
    positions = compute_positions(trd.clean, ins.clean, latest).cache()
    returns = daily_returns(prices_usd).cache()
    var = compute_var(positions, returns, cfg)
    exposure = compute_counterparty_exposure(trd.clean, ins.clean, cpt.clean, latest, fx.clean, cfg).cache()
    margin = compute_margin(exposure, cpt.clean, col.clean).cache()
    for name, df in (("positions", positions.orderBy(F.desc(F.abs("market_value_usd")))), ("var_summary", var),
                     ("counterparty_exposure", exposure), ("margin_calls", margin)):
        write_df_csv(df, out_dir / f"{name}.csv")

    # ---- 6. SQL reports ------------------------------------------------------------------------
    dq_df = spark.createDataFrame([{k: r[k] for k in DQ_FIELDS} for r in results])
    views = {"dq_results": dq_df, "recon": recon, "positions": positions, "returns_usd": returns,
             "var_summary": var, "counterparty_exposure": exposure, "margin": margin}
    for name, df in views.items():
        df.createOrReplaceTempView(name)
    sql_counts = run_sql_reports(spark, Path(__file__).resolve().parents[2] / "sql", out_dir / "sql", show=show)

    if show:
        print("\n== DQ dataset summary ==")
        for s in summary:
            print(f"{s['dataset']:<15} in={s['rows_in']:>6,}  clean={s['rows_clean']:>6,}  quarantined={s['rows_quarantined']:>5,}")
        print("\n== Reconciliation =="); summ.show(truncate=False)
        print("== VaR =="); var.show(truncate=False)
        print("== Counterparty exposure (top 5) =="); exposure.show(5, truncate=False)
        print("== Margin actions =="); margin.groupBy("margin_action").count().show()

    elapsed = round(time.time() - t0, 1)
    out = {"results": results, "summary": summary, "sql": sql_counts, "elapsed_s": elapsed,
           "spark": spark, "frames": {"recon": recon, "var": var, "exposure": exposure, "margin": margin,
                                      "positions": positions, "trades_clean": trd.clean}}
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the public-markets data platform pipeline")
    ap.add_argument("--as-of", default=Config().as_of.isoformat())
    ap.add_argument("--raw-dir", default=Config().raw_dir)
    ap.add_argument("--out-dir", default=Config().out_dir)
    ap.add_argument("--regen", action="store_true", help="regenerate synthetic raw feeds")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    cfg = replace(Config(), as_of=date.fromisoformat(a.as_of), raw_dir=a.raw_dir, out_dir=a.out_dir)
    res = run(cfg, regen=a.regen, show=0 if a.quiet else 20)
    bad = [r for r in res["results"] if r["status"] != "PASS"]
    print(f"\nDone in {res['elapsed_s']}s - {len(res['results'])} DQ rules evaluated, {len(bad)} not PASS. "
          f"Outputs in {cfg.out_dir}/")
    res["spark"].stop()


if __name__ == "__main__":
    main()
