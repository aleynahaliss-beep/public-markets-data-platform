"""Deterministic synthetic data generator.

Produces seven raw CSV feeds that mimic what a public-markets data team receives from upstream
systems (reference data, market data, front-office trades, back-office settlements, collateral).

All names, identifiers and prices are synthetic. Known data-quality faults are injected at fixed
positions so that every rule in the DQ framework has something to find and so that the test-suite
can assert detection counts against the injection plan.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
from datetime import date, timedelta
from pathlib import Path

from .calendar_utils import add_business_days, business_days
from .config import Config
from .validators import isin_check_digit

ALNUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
COUNTRIES = [("US", "USD"), ("GB", "GBP"), ("DE", "EUR"), ("FR", "EUR"), ("AE", "AED")]
SECTORS = ["Financials", "Technology", "Energy", "Healthcare", "Industrials", "Utilities", "Consumer"]
RATINGS = ["AA", "AA", "A", "A", "A", "BBB", "BBB", "BBB", "BB", "BB", "B", "B"]
# rating -> (threshold, minimum transfer amount, rounding) in USD.
# Terms are scaled to the size of the synthetic book (net exposures of roughly +/- USD 0.5m) so that
# the sample produces a realistic mix of margin calls, returns and "below MTA" outcomes.
CSA_TERMS = {
    "AA": (250_000, 50_000, 10_000),
    "A": (100_000, 25_000, 5_000),
    "BBB": (50_000, 25_000, 5_000),
    "BB": (0, 10_000, 5_000),
    "B": (0, 10_000, 5_000),
}
FLAWED_INSTRUMENTS = {"INS005", "INS009", "INS013", "INS030"}   # fail a CRITICAL reference-data rule

# Injection plan (documented in docs/03_data_dictionary.md). Indices refer to the sorted trade list.
TRADE_INJECTIONS = {
    "duplicate": [10, 50, 90],
    "settle_before_trade": [20, 60, 100, 140],
    "bad_quantity": {30: 0, 70: -500, 110: 0, 150: -100, 190: 0},
    "unknown_instrument": [40, 80, 120],
    "unknown_counterparty": [45, 85],
    "price_deviation": {25: 1.25, 65: 0.70, 105: 1.25, 145: 0.70},
    "bad_side": [55],
}
# Back-office breaks are picked deterministically from *eligible* trades (valid instrument, not
# cancelled, not already carrying a front-office fault) so each break is visible in the reconciliation.
BO_BREAK_PLAN = {            # name: (offset, stride, count)
    "missing_in_bo": (5, 53, 3),
    "quantity": (11, 53, 3),
    "amount": (17, 53, 3),
    "settle_date": (23, 53, 2),
}


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if row.get(k) is None else row.get(k)) for k in fields})


def _make_instruments(rng: random.Random, cfg: Config) -> tuple[list[dict], dict[str, tuple[float, float]]]:
    rows, params = [], {}
    for i in range(1, 41):
        asset_class = "EQUITY" if i <= 20 else "BOND" if i <= 32 else "ETF"
        country, ccy = rng.choice(COUNTRIES)
        body = country + "".join(rng.choices(ALNUM, k=9))
        iid = f"INS{i:03d}"
        if asset_class == "EQUITY":
            start, vol, sector, mult, maturity = rng.uniform(20, 400), 0.016, rng.choice(SECTORS), 1.0, None
        elif asset_class == "BOND":
            start, vol, sector, mult = rng.uniform(90, 105), 0.0025, rng.choice(["Government", "Corporate"]), 0.01
            maturity = cfg.as_of + timedelta(days=rng.randrange(730, 3650))
        else:
            start, vol, sector, mult, maturity = rng.uniform(30, 250), 0.010, "Fund", 1.0, None
        params[iid] = (start, vol)
        rows.append({
            "instrument_id": iid,
            "isin": body + str(isin_check_digit(body)),
            "ticker": f"TK{i:03d}",
            "instrument_name": f"Synthetic {asset_class.title()} {i:03d}",
            "asset_class": asset_class,
            "currency": ccy,
            "country": country,
            "sector": sector,
            "price_multiplier": mult,
            "maturity_date": maturity.isoformat() if maturity else "",
        })

    by_id = {r["instrument_id"]: r for r in rows}
    # --- injected reference-data faults -------------------------------------------------------
    bad = by_id["INS005"]["isin"]
    by_id["INS005"]["isin"] = bad[:-1] + str((int(bad[-1]) + 1) % 10)      # wrong check digit
    by_id["INS009"]["isin"] = by_id["INS008"]["isin"]                       # duplicate ISIN
    by_id["INS013"]["currency"] = ""                                        # missing currency
    by_id["INS017"]["sector"] = ""                                          # missing sector
    by_id["INS025"]["sector"] = ""
    by_id["INS030"]["asset_class"] = "EQUTY"                                # typo in asset class
    return rows, params


def _make_counterparties(rng: random.Random) -> list[dict]:
    rows = []
    for i, rating in enumerate(RATINGS):
        csa = "Y" if i < 9 else "N"
        thr, mta, rnd = CSA_TERMS[rating] if csa == "Y" else ("", "", "")
        rows.append({
            "counterparty_id": f"CP{i + 1:02d}",
            "counterparty_name": f"Counterparty {chr(65 + i)}",
            "lei": "".join(rng.choices(ALNUM, k=20)),
            "rating": rating,
            "country": rng.choice(["US", "GB", "DE", "AE", "FR"]),
            "csa_flag": csa,
            "threshold_usd": thr,
            "mta_usd": mta,
            "rounding_usd": rnd,
        })
    rows[6]["rating"] = ""                       # missing rating  (CP07)
    rows[9]["lei"] = rows[9]["lei"][:18]         # malformed LEI   (CP10)
    return rows


def _walk(rng: random.Random, start: float, vol: float, n: int) -> list[float]:
    p, out = start, []
    for _ in range(n):
        p *= math.exp(rng.gauss(0.0, vol))
        out.append(round(p, 4))
    return out


def generate(cfg: Config, out_dir: str | Path | None = None) -> dict[str, int]:
    rng = random.Random(cfg.seed)
    out = Path(out_dir or cfg.raw_dir)
    out.mkdir(parents=True, exist_ok=True)

    bdays = business_days(cfg.as_of, cfg.history_days)
    day_idx = {d: i for i, d in enumerate(bdays)}

    instruments, params = _make_instruments(rng, cfg)
    ins_by_id = {r["instrument_id"]: r for r in instruments}
    counterparties = _make_counterparties(rng)

    # ---- prices ------------------------------------------------------------------------------
    clean_px = {iid: _walk(rng, s, v, len(bdays)) for iid, (s, v) in params.items()}
    px = {iid: list(series) for iid, series in clean_px.items()}
    for iid, start in (("INS002", 200), ("INS014", 250)):                  # stale prices
        for k in range(start, start + 6):
            px[iid][k] = px[iid][start - 1]
    px["INS003"][100] = -px["INS003"][100]                                  # negative price
    px["INS011"][150] = 0.0                                                 # zero price
    px["INS006"][220] = round(px["INS006"][220] * 5, 4)                     # upward spike
    px["INS019"][120] = round(px["INS019"][120] * 0.2, 4)                   # downward spike
    missing = {("INS004", 180), ("INS004", 181), ("INS004", 182), ("INS022", 240)}

    price_rows = []
    for iid in params:
        ccy = ins_by_id[iid]["currency"] or dict(COUNTRIES)[ins_by_id[iid]["country"]]
        for k, d in enumerate(bdays):
            if (iid, k) in missing:
                continue
            price_rows.append({"price_date": d.isoformat(), "instrument_id": iid,
                               "close_price": px[iid][k], "price_currency": ccy})
    for k in (100, 101):                                                    # duplicate rows
        price_rows.append({"price_date": bdays[k].isoformat(), "instrument_id": "INS001",
                           "close_price": px["INS001"][k], "price_currency": ins_by_id["INS001"]["currency"]})

    # ---- FX ----------------------------------------------------------------------------------
    fx_rows = []
    for ccy, start, vol in (("USD", 1.0, 0.0), ("EUR", 1.08, 0.004), ("GBP", 1.27, 0.0045), ("AED", 0.2723, 0.0)):
        series = [start] * len(bdays) if vol == 0 else _walk(rng, start, vol, len(bdays))
        fx_rows += [{"rate_date": d.isoformat(), "currency": ccy, "usd_per_unit": round(series[k], 6)}
                    for k, d in enumerate(bdays)]

    # ---- trades ------------------------------------------------------------------------------
    trade_days = bdays[-cfg.trade_window_days:]
    drafts = []
    for _ in range(cfg.n_trades):
        tdate = rng.choice(trade_days)
        inst = rng.choice(instruments)
        iid = inst["instrument_id"]
        qty = rng.randrange(100_000, 3_000_001, 50_000) if inst["asset_class"] == "BOND" else rng.randrange(100, 5001, 100)
        price = round(clean_px[iid][day_idx[tdate]] * (1 + rng.gauss(0, 0.002)), 4)
        settle = add_business_days(tdate, 2 if rng.random() < 0.6 else rng.choice([20, 40, 60]))
        if rng.random() < 0.03:
            status = "CANCELLED"
        elif settle <= cfg.as_of:
            status = "SETTLED"
        else:
            status = rng.choice(["CONFIRMED", "CONFIRMED", "CONFIRMED", "CONFIRMED", "NEW"])
        drafts.append({
            "trade_date": tdate, "settle_date": settle, "instrument_id": iid,
            "counterparty_id": rng.choice(counterparties)["counterparty_id"],
            "side": rng.choice(["BUY", "SELL"]), "quantity": qty, "price": price,
            "currency": inst["currency"] or dict(COUNTRIES)[inst["country"]], "status": status,
        })
    drafts.sort(key=lambda t: (t["trade_date"], t["instrument_id"]))
    for n, t in enumerate(drafts, start=1):
        t["trade_id"] = f"T{n:06d}"

    true_trades = [dict(t) for t in drafts]          # what the back office will (mostly) agree with
    inj = TRADE_INJECTIONS
    for k in inj["settle_before_trade"]:
        drafts[k]["settle_date"] = drafts[k]["trade_date"] - timedelta(days=1)
    for k, q in inj["bad_quantity"].items():
        drafts[k]["quantity"] = q
    for k in inj["unknown_instrument"]:
        drafts[k]["instrument_id"] = f"INS9{k:02d}"
    for k in inj["unknown_counterparty"]:
        drafts[k]["counterparty_id"] = f"CP9{k:02d}"
    for k, f in inj["price_deviation"].items():
        drafts[k]["price"] = round(drafts[k]["price"] * f, 4)
    for k in inj["bad_side"]:
        drafts[k]["side"] = "HOLD"
    trades = list(drafts) + [dict(drafts[k]) for k in inj["duplicate"]]    # duplicates re-delivered at the end

    # ---- back-office settlements -------------------------------------------------------------
    injected_idx = set(inj["duplicate"]) | set(inj["settle_before_trade"]) | set(inj["bad_quantity"]) \
        | set(inj["unknown_instrument"]) | set(inj["unknown_counterparty"]) | set(inj["price_deviation"]) \
        | set(inj["bad_side"])
    eligible = [k for k, t in enumerate(true_trades)
                if t["instrument_id"] not in FLAWED_INSTRUMENTS and t["status"] != "CANCELLED"
                and k not in injected_idx]
    plan = {name: {true_trades[k]["trade_id"] for k in eligible[off::stride][:cnt]}
            for name, (off, stride, cnt) in BO_BREAK_PLAN.items()}
    breaks_missing, breaks_qty = plan["missing_in_bo"], plan["quantity"]
    breaks_amt, breaks_date = plan["amount"], plan["settle_date"]
    settlements = []
    for t in true_trades:
        if t["status"] == "CANCELLED" or t["trade_id"] in breaks_missing:
            continue
        mult = float(ins_by_id[t["instrument_id"]]["price_multiplier"])
        qty = t["quantity"] + (100 if t["trade_id"] in breaks_qty else 0)
        amount = round(qty * mult * t["price"], 2)
        if t["trade_id"] in breaks_amt:
            amount = round(amount * 1.003, 2)
        settle = add_business_days(t["settle_date"], 1) if t["trade_id"] in breaks_date else t["settle_date"]
        settlements.append({"trade_id": t["trade_id"], "settle_date": settle.isoformat(), "quantity": qty,
                            "net_amount": amount, "currency": t["currency"],
                            "status": "SETTLED" if settle <= cfg.as_of else "PENDING"})
    for orphan in ("T900001", "T900002"):                                   # BO-only records
        settlements.append({"trade_id": orphan, "settle_date": cfg.as_of.isoformat(), "quantity": 1000,
                            "net_amount": 25_000.0, "currency": "USD", "status": "SETTLED"})

    # ---- collateral --------------------------------------------------------------------------
    collateral = []
    for i, cp in enumerate(counterparties):
        if cp["csa_flag"] != "Y":
            continue
        held = 0 if i == 4 else rng.randrange(0, 120_001, 5_000)
        as_of = cfg.as_of - timedelta(days=5) if i == 3 else cfg.as_of      # stale balance (CP04)
        collateral.append({"counterparty_id": cp["counterparty_id"], "collateral_as_of": as_of.isoformat(),
                           "collateral_held_usd": held})

    # ---- write -------------------------------------------------------------------------------
    for t in trades:
        t["trade_date"], t["settle_date"] = t["trade_date"].isoformat(), t["settle_date"].isoformat()
    _write_csv(out / "instruments.csv", list(instruments[0].keys()), instruments)
    _write_csv(out / "counterparties.csv", list(counterparties[0].keys()), counterparties)
    _write_csv(out / "prices.csv", ["price_date", "instrument_id", "close_price", "price_currency"], price_rows)
    _write_csv(out / "fx_rates.csv", ["rate_date", "currency", "usd_per_unit"], fx_rows)
    _write_csv(out / "trades.csv", ["trade_id", "trade_date", "settle_date", "instrument_id", "counterparty_id",
                                    "side", "quantity", "price", "currency", "status"], trades)
    _write_csv(out / "settlements.csv", list(settlements[0].keys()), settlements)
    _write_csv(out / "collateral.csv", list(collateral[0].keys()), collateral)
    return {"instruments": len(instruments), "counterparties": len(counterparties), "prices": len(price_rows),
            "fx_rates": len(fx_rows), "trades": len(trades), "settlements": len(settlements),
            "collateral": len(collateral)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic raw feeds")
    parser.add_argument("--out", default=Config().raw_dir)
    parser.add_argument("--seed", type=int, default=Config().seed)
    args = parser.parse_args()
    counts = generate(Config(seed=args.seed), args.out)
    for name, n in counts.items():
        print(f"{name:<15}{n:>8,}")


if __name__ == "__main__":
    main()
