# 02 - User stories and acceptance criteria

Format: *As a ... I want ... so that ...*, followed by Given / When / Then acceptance criteria.
Each story is traced to a requirement in the [BRD](01_business_requirements.md) and to the test that proves it.

---

### US-01 Trust only validated data
**As a** risk analyst **I want** rows that break a critical rule to be kept out of the risk calculations
**so that** one bad record cannot distort VaR, exposure or margin.

- **Given** a trade with quantity 0, **when** the DQ gate runs, **then** it appears in `quarantine/trades.csv` and not in positions, exposure or reconciliation as a live trade.
- **Given** a row that only breaks a WARNING rule (e.g. missing sector), **when** the gate runs, **then** it stays in the clean set and the breach is visible in `dq_results.csv`.
- **Given** any dataset, **then** `rows_clean + rows_quarantined = rows_in`.

*Traces to:* FR-06 - *Test:* `test_clean_plus_quarantine_equals_input`, `test_instrument_rules`

---

### US-02 Explain every rejection
**As a** data steward **I want** each quarantined row to list every rule it broke
**so that** I can send the upstream owner a precise, actionable defect.

- **Given** a trade that fails two rules (unknown instrument and unknown counterparty), **then** `dq_failed_rules` contains both ids, separated by `|`.
- **Given** a re-delivered trade id, **then** the first delivery is accepted and only the repeat is quarantined (TRD_002).

*Traces to:* FR-06, NFR-01 - *Test:* `test_trade_rules_catch_each_fault`

---

### US-03 Catch fat-finger and mis-priced trades
**As a** front-office supervisor **I want** trades priced far from the market to be flagged
**so that** erroneous bookings are fixed before they create phantom P&L and exposure.

- **Given** a trade priced 20% above the trade-date close, **when** the gate runs, **then** TRD_009 rejects it.
- **Given** a trade priced 0.2% from the close, **then** it passes.
- **Given** no close exists for that instrument and date, **then** the rule is not evaluated (never fails by accident).

*Traces to:* FR-05 - *Test:* `test_trade_rules_catch_each_fault`

---

### US-04 Keep bad prices out of VaR
**As a** market-risk manager **I want** bad ticks removed from the return history
**so that** a single spike does not appear as a 400% tail loss.

- **Given** a close that is 5x its neighbours on both sides, **then** PRC_004 quarantines it.
- **Given** zero or negative closes, **then** PRC_001 quarantines them.
- **Given** five identical closes in a row, **then** PRC_005 warns (feed may be dead) but keeps the rows.
- **Given** a business day without a price, **then** PRC_006 reports it and lists the instrument/date pairs in `quarantine/prices_missing_dates.csv`.

*Traces to:* FR-04 - *Test:* `test_price_rules_spike_stale_negative_duplicate`

---

### US-05 Reconcile front office to back office
**As an** operations analyst **I want** every trade classified against the settlement record
**so that** I only work real breaks.

- **Given** matching trade and settlement, **then** category is `MATCHED` (amount differences within tolerance count as matched).
- **Given** different quantities, amounts or settle dates, **then** the category is `QUANTITY_BREAK`, `AMOUNT_BREAK` or `SETTLE_DATE_BREAK` (first match wins in that order).
- **Given** a live trade without a settlement instruction, **then** `MISSING_IN_BACK_OFFICE`; **given** a settlement with no trade, **then** `MISSING_IN_FRONT_OFFICE`.
- **Given** a settlement whose front-office trade was rejected by the DQ gate, **then** `FO_REJECTED_BY_DQ` - a data problem, not a settlement problem.
- **Given** a cancelled trade, **then** it is not expected in the back office.

*Traces to:* FR-08 - *Test:* `test_every_break_category`, `test_amount_within_tolerance_is_a_match`, `test_reconciliation_matches_injection_plan`

---

### US-06 Know my market risk
**As a** market-risk manager **I want** 1-day 99% VaR and Expected Shortfall for the portfolio and each asset class
**so that** I can compare it with limits and see diversification.

- **Given** a USD 1m position and 101 historical returns from -5.0% to +5.0%, **then** VaR = 49,000 and ES = 49,500.
- **Given** a short position, **then** losses arise on up-moves (sign handled).
- **Given** the sample book, **then** portfolio VaR is below the sum of the asset-class VaRs (diversification) and ES >= VaR.

*Traces to:* FR-09, FR-10 - *Test:* `test_historical_var_and_es_are_exact`, `test_short_position_has_losses_on_up_moves`, `test_positions_exclude_cancelled_and_sign_sells`, `test_risk_outputs_are_sane`

---

### US-07 See counterparty exposure with netting
**As a** credit-risk officer **I want** exposure per counterparty after netting
**so that** I can see where we are most exposed and how concentrated the book is.

- **Given** one BUY with +10,000 MtM and one SELL with -2,500 MtM on the same counterparty, **then** replacement cost = 7,500 (not 10,000).
- **Given** settled or cancelled trades, **then** they are excluded from exposure.
- **Given** notional 165,000, equity factor 20%, 1 year to settlement and NGR 0.75, **then** PFE add-on = 28,050 and EAD estimate = 35,550.
- **Given** the exposure table, **then** SQL report 04 shows each counterparty's share and the cumulative share of total EAD.

*Traces to:* FR-11, FR-13 - *Test:* `test_exposure_netting_and_addon`

---

### US-08 Call and return the right collateral
**As a** collateral manager **I want** margin calls and returns computed from the CSA terms
**so that** we neither under-collateralise nor trigger needless disputes.

- **Given** net MtM 389,290, threshold 100,000, collateral held 70,000, MTA 25,000, rounding 5,000, **then** call = 220,000 (shortfall 219,290 rounded **up**).
- **Given** a shortfall of 12,520 (below MTA), **then** `NO_ACTION`.
- **Given** 105,000 held against ~0 exposure, **then** return = 105,000 (rounded **down**).
- **Given** a counterparty without a CSA, **then** `NO_CSA` and its positive exposure is reported as uncollateralised.

*Traces to:* FR-12 - *Test:* `test_margin_call_rounding_mta_and_returns`

---

### US-09 Measure data quality for management
**As a** head of data **I want** a scorecard by rule and by quality dimension
**so that** I can prioritise fixes and track improvement over time.

- **Given** a run, **then** `dq_results.csv` has one row per rule with status PASS / WARN / FAIL.
- **Given** the results, **then** SQL 01 ranks failing rules inside each dataset and SQL 02 gives a pass-rate by dimension.

*Traces to:* FR-07, FR-13 - *Test:* `test_all_sql_reports_produce_output`

---

### US-10 Add a rule without touching the engine
**As a** data engineer **I want** rules to be declarative
**so that** a new control is one entry plus one test and shows up in the documentation automatically.

- **Given** a new `Rule(...)` appended to a dataset's rule list, **then** the engine evaluates it in the same single pass, results include it, and `scripts/gen_rule_matrix.py` lists it in the rule matrix.

*Traces to:* FR-16, NFR-03 - *Test:* rule matrix generation + `test_injected_faults_are_all_detected`

---

### US-11 Make the identifier check production-grade
**As a** data engineer **I want** the ISIN check digit validated with native Spark expressions
**so that** it runs without Python workers and is provably equivalent to the reference algorithm.

- **Given** 900+ valid, mutated and malformed identifiers, **then** the Spark expression and the Python reference agree on every one.

*Traces to:* FR-02, NFR-04 - *Test:* `test_spark_isin_expression_agrees_with_python_reference`
