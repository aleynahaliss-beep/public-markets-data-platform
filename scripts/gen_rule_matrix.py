"""Generate docs/04_dq_rules_matrix.md from the rule definitions so documentation cannot drift from code.

    python scripts/gen_rule_matrix.py
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pmdp.config import Config  # noqa: E402
from pmdp.dq_rules import catalogue  # noqa: E402

DATASET_ORDER = ["instruments", "counterparties", "prices", "fx_rates", "trades", "settlements", "collateral"]


def main() -> None:
    cfg = Config()
    rules = catalogue(cfg)
    by_sev = Counter(r.severity for r in rules)
    lines = [
        "# 04 - Data-quality rule matrix",
        "",
        "> Generated from `src/pmdp/dq_rules.py` by `scripts/gen_rule_matrix.py` - do not edit by hand.",
        "",
        f"**{len(rules)} rules** across {len(DATASET_ORDER)} datasets "
        f"({by_sev['CRITICAL']} CRITICAL, {by_sev['WARNING']} WARNING).",
        "",
        "| Severity | Behaviour |",
        "|---|---|",
        "| CRITICAL | Row is moved to `output/quarantine/<dataset>.csv` with the ids of the rules it broke; it never reaches reconciliation or risk. |",
        "| WARNING  | Row is kept, the breach is reported in `dq_results.csv` and the data owner is expected to fix it upstream. |",
        "",
        "Status per rule: `PASS` (failure rate within tolerance), `FAIL` (CRITICAL rule breached), `WARN` (WARNING rule breached).",
        "",
        "Thresholds in force: price spike "
        f"{int(cfg.price_spike_threshold * 100)}%, trade-price tolerance {int(cfg.trade_price_tolerance * 100)}%, "
        f"stale price = {cfg.stale_price_days} identical closes, reconciliation amount tolerance {cfg.recon_amount_tolerance:.2f}.",
        "",
    ]
    for ds in DATASET_ORDER:
        ds_rules = [r for r in rules if r.dataset == ds]
        lines += [f"## {ds}", "", "| Rule | Check | Dimension | Severity | Why it matters |", "|---|---|---|---|---|"]
        for r in ds_rules:
            lines.append(f"| `{r.rule_id}` | {r.name} | {r.dimension} | {r.severity} | {r.rationale} |")
        lines.append("")
    out = ROOT / "docs" / "04_dq_rules_matrix.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {out.relative_to(ROOT)} ({len(rules)} rules)")


if __name__ == "__main__":
    main()
