"""Central configuration and reference constants for the platform."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

ALLOWED_CURRENCIES = ("USD", "EUR", "GBP", "AED")
ASSET_CLASSES = ("EQUITY", "BOND", "ETF")
TRADE_SIDES = ("BUY", "SELL")
TRADE_STATUSES = ("NEW", "CONFIRMED", "SETTLED", "CANCELLED")
OPEN_STATUSES = ("NEW", "CONFIRMED")

# Illustrative potential-future-exposure add-on factors by asset class.
# These are NOT regulatory supervisory factors (SA-CCR); they exist to demonstrate the mechanics.
ADDON_FACTORS = {"EQUITY": 0.20, "ETF": 0.15, "BOND": 0.05}

# Netting benefit weights used in the CEM-style netting adjustment: PFE = addon * (0.4 + 0.6 * NGR)
NETTING_FLOOR = 0.4
NETTING_WEIGHT = 0.6


@dataclass(frozen=True)
class Config:
    """Run-time parameters. All thresholds that drive a business rule live here."""

    as_of: date = date(2026, 11, 16)
    seed: int = 42
    history_days: int = 300          # business days of price history to generate
    trade_window_days: int = 120     # trades are booked over the last N business days
    n_trades: int = 700

    # Market risk
    var_confidence: float = 0.99
    var_window: int = 250            # number of historical return scenarios

    # Data quality thresholds
    price_spike_threshold: float = 0.40       # isolated one-day move that reverts
    trade_price_tolerance: float = 0.10       # trade price vs. close on trade date
    stale_price_days: int = 5                 # identical closes in a row

    # Reconciliation
    recon_amount_tolerance: float = 0.50      # absolute, in trade currency

    raw_dir: str = "data/raw"
    out_dir: str = "output"
