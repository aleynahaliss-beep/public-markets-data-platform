# 03 - Data dictionary

All feeds are CSV files in `data/raw/`, read with explicit schemas (`src/pmdp/ingest.py`). Every feed
also receives an `ingest_seq` column (file order) so "first delivery wins" is deterministic.
Rule ids refer to the [rule matrix](04_dq_rules_matrix.md).

## instruments (reference data)
| Field | Type | Description | Rules |
|---|---|---|---|
| instrument_id | string | Internal security key, `INSnnn` | INS_001, INS_002 |
| isin | string | ISO 6166 identifier (2 letters + 9 alphanumerics + check digit) | INS_003, INS_004 |
| ticker | string | Short display code | - |
| instrument_name | string | Synthetic descriptive name | - |
| asset_class | string | `EQUITY`, `BOND`, `ETF` | INS_007 |
| currency | string | Listing currency: `USD`, `EUR`, `GBP`, `AED` | INS_005, INS_006 |
| country | string | ISO country of issue | - |
| sector | string | Sector / bond type / `Fund` | INS_008 (warning) |
| price_multiplier | double | Value per unit of quantity per unit of price (equity/ETF 1.0, bond 0.01 = price per 100 face) | INS_010 |
| maturity_date | date | Bonds only | INS_009 |

## counterparties (reference data)
| Field | Type | Description | Rules |
|---|---|---|---|
| counterparty_id | string | Key, `CPnn` | CPT_001 |
| counterparty_name | string | Synthetic name | - |
| lei | string | Legal Entity Identifier, 20 upper-case alphanumerics (synthetic, not real LEIs) | CPT_002 (warning) |
| rating | string | `AA`, `A`, `BBB`, `BB`, `B` | CPT_003 (warning) |
| country | string | Country of domicile | - |
| csa_flag | string | `Y` if a margin agreement exists, otherwise `N` | CPT_004, CPT_005 |
| threshold_usd | double | Unsecured exposure allowed before collateral is due (CSA only) | CPT_005 |
| mta_usd | double | Minimum transfer amount (CSA only) | CPT_005 |
| rounding_usd | double | Rounding increment for calls and returns (CSA only) | CPT_005 |

## prices (market data, daily close in listing currency)
| Field | Type | Description | Rules |
|---|---|---|---|
| price_date | date | Business day | PRC_003, PRC_006 |
| instrument_id | string | Key to instruments | PRC_002 |
| close_price | double | Closing price | PRC_001, PRC_004, PRC_005 |
| price_currency | string | Currency of the quote | - |

## fx_rates (market data)
| Field | Type | Description | Rules |
|---|---|---|---|
| rate_date | date | Business day | FXR_002 |
| currency | string | Currency being converted | FXR_002 |
| usd_per_unit | double | USD value of one unit of `currency` | FXR_001 |

## trades (front office)
| Field | Type | Description | Rules |
|---|---|---|---|
| trade_id | string | Key, `Tnnnnnn` | TRD_001, TRD_002 |
| trade_date | date | Execution date | TRD_008, TRD_009 |
| settle_date | date | Contractual settlement date (T+2 for ~60% of trades, 20/40/60 business days for the rest) | TRD_005 |
| instrument_id | string | Key to instruments | TRD_006 |
| counterparty_id | string | Key to counterparties | TRD_007 |
| side | string | `BUY` or `SELL` | TRD_003 |
| quantity | double | Units (equity/ETF) or face amount (bond); always positive | TRD_004 |
| price | double | Execution price in trade currency | TRD_009 |
| currency | string | Trade currency | TRD_010 |
| status | string | `NEW`, `CONFIRMED`, `SETTLED`, `CANCELLED` | TRD_011 |

"Open" trades (for exposure) = status `NEW` or `CONFIRMED` **and** `settle_date > as_of`.

## settlements (back office)
| Field | Type | Description | Rules |
|---|---|---|---|
| trade_id | string | Key to trades | SET_001, SET_002 |
| settle_date | date | Settlement date held by operations | - |
| quantity | double | Quantity held by operations | SET_003 |
| net_amount | double | `quantity x multiplier x price` in trade currency | - |
| currency | string | Settlement currency | - |
| status | string | `SETTLED` or `PENDING` | - |

## collateral (collateral management)
| Field | Type | Description | Rules |
|---|---|---|---|
| counterparty_id | string | Key to counterparties | COL_001 |
| collateral_as_of | date | Balance date | COL_003 (warning) |
| collateral_held_usd | double | Collateral currently held from the counterparty | COL_002 |

---

## Output catalogue (`output/`)

| File | Grain | Content |
|---|---|---|
| `dq_results.csv` | rule | rows tested, failures, rate, tolerance, PASS/WARN/FAIL |
| `dq_dataset_summary.csv` | dataset | rows in / clean / quarantined |
| `quarantine/<dataset>.csv` | row | rejected rows + `dq_failed_rules` |
| `quarantine/prices_missing_dates.csv` | instrument x date | business days with no price (PRC_006) |
| `recon_trades.csv`, `recon_summary.csv` | trade / category | reconciliation detail and roll-up |
| `positions.csv` | instrument | net quantity, latest USD price, USD market value |
| `var_summary.csv` | scope | 1-day 99% VaR, ES, worst scenario P&L |
| `counterparty_exposure.csv` | counterparty | open trades, notional, MtM, RC, PFE add-on, EAD estimate |
| `margin_calls.csv` | counterparty | required collateral, held, call / return, action, uncollateralised exposure |
| `sql/01..07_*.csv` | per report | analyst-facing SQL results |

---

## Injection plan (what the synthetic feeds deliberately get wrong)

The generator injects these faults at fixed positions so each control has something to find. The
end-to-end test asserts that the listed rule reports exactly the stated number of failing rows.

| Dataset | Fault | Count | Detected by |
|---|---|---|---|
| instruments | ISIN with wrong check digit | 1 | INS_003 |
| instruments | ISIN duplicated from another instrument | 1 | INS_004 |
| instruments | Missing currency | 1 | INS_005 |
| instruments | Asset class typo (`EQUTY`) | 1 | INS_007 |
| instruments | Missing sector | 2 | INS_008 |
| counterparties | Malformed LEI (18 characters) | 1 | CPT_002 |
| counterparties | Missing rating | 1 | CPT_003 |
| prices | Zero / negative close | 2 | PRC_001 |
| prices | Duplicate rows | 2 | PRC_003 |
| prices | Isolated spike (x5 and x0.2) | 2 | PRC_004 |
| prices | Frozen price (6 days, 2 instruments; 3 flagged rows each) | 6 | PRC_005 |
| prices | Missing business days | 4 | PRC_006 |
| trades | Re-delivered trade id | 3 | TRD_002 |
| trades | Side `HOLD` | 1 | TRD_003 |
| trades | Zero / negative quantity | 5 | TRD_004 |
| trades | Settlement before trade date | 4 | TRD_005 |
| trades | Unknown counterparty | 2 | TRD_007 |
| trades | Price 25% above / 30% below close | 4 | TRD_009 |
| trades | Unknown instrument id (3) **plus** all trades on the 4 quarantined instruments (67 on the sample) | 70 | TRD_006 |
| collateral | Stale balance date | 1 | COL_003 |
| settlements | Missing in back office / quantity / amount / settle-date / back-office-only | 3 / 3 / 3 / 2 / 2 | reconciliation categories |

The last trades row shows the **cascade effect** of reference-data errors: 4 bad instrument records
(10% of the master) cause 67 trades (9.5% of the blotter) to be rejected - 76% of everything that lands in
the trades quarantine has that single root cause.
