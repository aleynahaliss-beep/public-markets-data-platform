# 05 - Process flow and data lineage

## End-to-end flow

```mermaid
flowchart LR
    subgraph RAW["Raw feeds (CSV)"]
        I[instruments]
        C[counterparties]
        P[prices]
        F[fx_rates]
        T[trades]
        S[settlements]
        K[collateral]
    end

    subgraph GATE["Data-quality gate (40 rules)"]
        direction TB
        G1["1. Reference data<br/>instruments, counterparties"]
        G2["2. Market data<br/>prices, FX"]
        G3["3. Transactions<br/>trades, settlements, collateral<br/>(integrity vs validated masters)"]
        G1 --> G2 --> G3
    end

    I --> G1
    C --> G1
    P --> G2
    F --> G2
    T --> G3
    S --> G3
    K --> G3

    G3 -->|CRITICAL breach| Q[("quarantine/*.csv<br/>+ failing rule ids")]
    G3 -->|clean rows| CL[Validated datasets]
    G3 --> DQ[("dq_results.csv<br/>dq_dataset_summary.csv")]

    CL --> REC["Reconciliation<br/>front office vs back office"]
    Q -. rejected trade ids .-> REC
    CL --> POS["Positions<br/>USD market value"]
    POS --> VAR["Historical-simulation<br/>VaR / ES"]
    CL --> EXP["Counterparty exposure<br/>netting, RC, PFE, EAD"]
    EXP --> MRG["Margin<br/>call / return"]

    REC --> OUT[("output/*.csv")]
    VAR --> OUT
    EXP --> OUT
    MRG --> OUT
    OUT --> SQL["7 Spark SQL reports<br/>sql/*.sql"]
```

## Why the gate runs in three stages

Each stage validates against the **already validated** output of the previous one:

1. **Reference data first.** A bad instrument must be rejected before anything can reference it.
2. **Market data second.** Prices are checked against the raw instrument ids (existence only) so one root cause is not counted twice.
3. **Transactions last.** Trades and collateral are checked for referential integrity against the *validated* instrument and counterparty masters, and for price sanity against the validated prices.

The consequence is intentional and visible in the results: a defect in a master record cascades to every
transaction that points at it (4 bad instruments -> 67 rejected trades on the sample), and the
reconciliation labels the matching back-office records `FO_REJECTED_BY_DQ` so operations do not chase them
as settlement breaks.

## Rule outcome logic

```mermaid
flowchart TD
    R[Row evaluated against every rule of its dataset] --> A{Any CRITICAL rule failed?}
    A -- yes --> Q["Quarantine<br/>dq_failed_rules = all failed CRITICAL ids"]
    A -- no --> C["Clean set"]
    C --> W{Any WARNING rule failed?}
    W -- yes --> RW["Stays in clean set;<br/>breach counted in dq_results.csv"]
    W -- no --> OK[No finding]
```

A rule predicate that evaluates to NULL (cannot be evaluated, e.g. no close price for the trade date)
never fails a row.

## Reconciliation decision order

```mermaid
flowchart TD
    S[Full outer join on trade_id] --> A{In front office?}
    A -- no --> B{Front-office row rejected by DQ?}
    B -- yes --> FR[FO_REJECTED_BY_DQ]
    B -- no --> MF[MISSING_IN_FRONT_OFFICE]
    A -- yes --> C{In back office?}
    C -- no --> MB[MISSING_IN_BACK_OFFICE]
    C -- yes --> D{Quantity equal?}
    D -- no --> QB[QUANTITY_BREAK]
    D -- yes --> E{"Amount within tolerance?"}
    E -- no --> AB[AMOUNT_BREAK]
    E -- yes --> F{Settle date equal?}
    F -- no --> DB[SETTLE_DATE_BREAK]
    F -- yes --> M[MATCHED]
```

## Margin decision logic

```mermaid
flowchart TD
    S[Counterparty] --> A{CSA = Y?}
    A -- no --> N["NO_CSA<br/>positive exposure reported as uncollateralised"]
    A -- yes --> R["required = max(net MtM - threshold, 0)<br/>raw = required - collateral held"]
    R --> M{"abs(raw) >= MTA?"}
    M -- no --> NA[NO_ACTION]
    M -- yes --> D{raw > 0?}
    D -- yes --> MC["MARGIN_CALL<br/>rounded UP to increment"]
    D -- no --> CR["COLLATERAL_RETURN<br/>rounded DOWN to increment"]
```

## Run order and outputs

| Step | Module | Output |
|---|---|---|
| Generate (optional) | `generate_data.py` | `data/raw/*.csv` |
| Ingest | `ingest.py` | staging DataFrames with `ingest_seq` |
| DQ gate | `dq_rules.py`, `data_quality.py` | `dq_results.csv`, `dq_dataset_summary.csv`, `quarantine/` |
| Reconcile | `reconciliation.py` | `recon_trades.csv`, `recon_summary.csv` |
| Risk | `risk.py` | `positions.csv`, `var_summary.csv`, `counterparty_exposure.csv`, `margin_calls.csv` |
| SQL reports | `sql_reports.py` + `sql/*.sql` | `sql/*.csv` |
