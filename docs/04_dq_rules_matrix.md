# 04 - Data-quality rule matrix

> Generated from `src/pmdp/dq_rules.py` by `scripts/gen_rule_matrix.py` - do not edit by hand.

**40 rules** across 7 datasets (34 CRITICAL, 6 WARNING).

| Severity | Behaviour |
|---|---|
| CRITICAL | Row is moved to `output/quarantine/<dataset>.csv` with the ids of the rules it broke; it never reaches reconciliation or risk. |
| WARNING  | Row is kept, the breach is reported in `dq_results.csv` and the data owner is expected to fix it upstream. |

Status per rule: `PASS` (failure rate within tolerance), `FAIL` (CRITICAL rule breached), `WARN` (WARNING rule breached).

Thresholds in force: price spike 40%, trade-price tolerance 10%, stale price = 5 identical closes, reconciliation amount tolerance 0.50.

## instruments

| Rule | Check | Dimension | Severity | Why it matters |
|---|---|---|---|---|
| `INS_001` | Instrument ID is populated | Completeness | CRITICAL | Primary key - without it nothing downstream can join. |
| `INS_002` | Instrument ID is unique | Uniqueness | CRITICAL | Duplicate keys double-count positions and exposures. |
| `INS_003` | ISIN has valid format and ISO 6166 check digit | Validity | CRITICAL | A mistyped ISIN breaks look-ups against vendor and regulatory feeds. |
| `INS_004` | ISIN is unique across instruments | Uniqueness | CRITICAL | Two instruments sharing an ISIN make security identification ambiguous. |
| `INS_005` | Currency is populated | Completeness | CRITICAL | Currency drives FX conversion of every price and trade. |
| `INS_006` | Currency is in the supported ISO 4217 list | Validity | CRITICAL | Unsupported currency has no FX rate. |
| `INS_007` | Asset class is a recognised value | Validity | CRITICAL | Add-on factors and risk buckets key on asset class. |
| `INS_008` | Sector is populated | Completeness | WARNING | Needed for concentration reporting; not blocking for pricing. |
| `INS_009` | Bonds carry a future maturity date | Consistency | CRITICAL | Matured or undated bonds should not be live in the master. |
| `INS_010` | Price multiplier is positive | Validity | CRITICAL | A zero or negative multiplier zeroes out or flips valuations. |

## counterparties

| Rule | Check | Dimension | Severity | Why it matters |
|---|---|---|---|---|
| `CPT_001` | Counterparty ID is populated and unique | Uniqueness | CRITICAL | Primary key of the netting-set owner. |
| `CPT_002` | LEI is 20 upper-case alphanumeric characters | Validity | WARNING | LEI is required for regulatory reporting. |
| `CPT_003` | Credit rating is populated | Completeness | WARNING | Rating drives limits and reporting buckets. |
| `CPT_004` | csa_flag is Y or N | Validity | CRITICAL | Determines whether margining applies. |
| `CPT_005` | CSA counterparties carry threshold, MTA and rounding | Completeness | CRITICAL | Margin calls cannot be computed without CSA terms. |

## prices

| Rule | Check | Dimension | Severity | Why it matters |
|---|---|---|---|---|
| `PRC_001` | Close price is present and positive | Validity | CRITICAL | Zero / negative prices corrupt returns and valuations. |
| `PRC_002` | Price references a known instrument | Integrity | CRITICAL | Orphan prices cannot be attributed to a security. |
| `PRC_003` | One price per instrument per date | Uniqueness | CRITICAL | Duplicates distort return series and VaR. |
| `PRC_004` | No isolated price spike (> 40% move that reverses) | Accuracy | CRITICAL | Bad ticks create phantom tail losses in historical-simulation VaR. |
| `PRC_005` | No stale price (5 identical closes in a row) | Timeliness | WARNING | A frozen price usually means a dead feed, not a flat market. |
| `PRC_006` | Price exists for every instrument on every business day | Completeness | WARNING | Gaps force multi-day returns and understate daily volatility. |

## fx_rates

| Rule | Check | Dimension | Severity | Why it matters |
|---|---|---|---|---|
| `FXR_001` | FX rate is positive | Validity | CRITICAL | Non-positive rate. |
| `FXR_002` | One rate per currency per date | Uniqueness | CRITICAL | Duplicate rates make conversions non-deterministic. |

## trades

| Rule | Check | Dimension | Severity | Why it matters |
|---|---|---|---|---|
| `TRD_001` | Trade ID is populated | Completeness | CRITICAL | Primary key for reconciliation. |
| `TRD_002` | Trade ID is unique (first delivery wins) | Uniqueness | CRITICAL | Re-delivered trades would be booked twice. |
| `TRD_003` | Side is BUY or SELL | Validity | CRITICAL | Direction sets the sign of position and exposure. |
| `TRD_004` | Quantity is positive | Validity | CRITICAL | Direction is carried by side, not sign. |
| `TRD_005` | Settlement date is on or after trade date | Consistency | CRITICAL | Time-travelling settlement. |
| `TRD_006` | Instrument exists in the validated instrument master | Integrity | CRITICAL | Cannot value a trade on an unknown / quarantined security. |
| `TRD_007` | Counterparty exists in the validated counterparty master | Integrity | CRITICAL | Cannot attach exposure to an unknown legal entity. |
| `TRD_008` | Trade date is not in the future | Validity | CRITICAL | Future-dated trade in the blotter. |
| `TRD_009` | Price within 10% of trade-date close | Accuracy | CRITICAL | Fat-finger / mis-priced trade would distort MtM and exposure. |
| `TRD_010` | Trade currency matches instrument currency | Consistency | CRITICAL | Currency mismatch breaks FX conversion. |
| `TRD_011` | Status is a recognised lifecycle value | Validity | CRITICAL | Open / closed logic keys on status. |

## settlements

| Rule | Check | Dimension | Severity | Why it matters |
|---|---|---|---|---|
| `SET_001` | Trade ID is populated | Completeness | CRITICAL | Join key for reconciliation. |
| `SET_002` | One settlement record per trade | Uniqueness | CRITICAL | Duplicate instructions risk double settlement. |
| `SET_003` | Settled quantity is positive | Validity | CRITICAL | Empty settlement instruction. |

## collateral

| Rule | Check | Dimension | Severity | Why it matters |
|---|---|---|---|---|
| `COL_001` | Counterparty exists in the validated master | Integrity | CRITICAL | Collateral must belong to a known legal entity. |
| `COL_002` | Collateral balance is non-negative | Validity | CRITICAL | Negative balance. |
| `COL_003` | Collateral balance is as of the run date | Timeliness | WARNING | A stale balance can trigger a wrong margin call. |
