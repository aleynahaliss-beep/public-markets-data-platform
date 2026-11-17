"""Declarative data-quality engine.

A `Rule` describes *what is wrong* (a failure predicate), not how to run it. The engine evaluates
all rules of a dataset in a single pass, then

* routes rows that breach a CRITICAL rule to a quarantine table (with the rule ids that caught them),
* keeps rows that only breach WARNING rules in the clean table (they are reported, not removed),
* returns one result record per rule with counts, rate and PASS / WARN / FAIL status.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

from pyspark.sql import Column, DataFrame, functions as F

CRITICAL, WARNING = "CRITICAL", "WARNING"

# Standard DQ dimensions used in the rule catalogue
COMPLETENESS, VALIDITY, UNIQUENESS = "Completeness", "Validity", "Uniqueness"
CONSISTENCY, ACCURACY, TIMELINESS, INTEGRITY = "Consistency", "Accuracy", "Timeliness", "Integrity"


@dataclass(frozen=True)
class Rule:
    rule_id: str
    dataset: str
    name: str
    dimension: str
    severity: str                      # CRITICAL -> quarantine, WARNING -> report only
    fail: Callable[[], Column]         # lazily built so the catalogue can be listed without Spark
    rationale: str = ""
    max_fail_rate: float = 0.0         # tolerated share of failing rows before status != PASS


@dataclass
class DQOutcome:
    clean: DataFrame
    quarantine: DataFrame
    results: list[dict]
    total_rows: int
    quarantined_rows: int


def result_record(rule: Rule, total: int, failed: int) -> dict:
    rate = failed / total if total else 0.0
    if rate <= rule.max_fail_rate:
        status = "PASS"
    else:
        status = "FAIL" if rule.severity == CRITICAL else "WARN"
    return {
        "dataset": rule.dataset, "rule_id": rule.rule_id, "rule_name": rule.name,
        "dimension": rule.dimension, "severity": rule.severity, "total_rows": total,
        "failed_rows": failed, "fail_rate": round(rate, 6), "max_fail_rate": rule.max_fail_rate,
        "status": status,
    }


def _id_array(rules: Sequence[Rule]) -> Column:
    if not rules:
        return F.array().cast("array<string>")
    flags = [F.when(F.col(f"_f_{r.rule_id}"), F.lit(r.rule_id)) for r in rules]
    return F.filter(F.array(*flags), lambda x: x.isNotNull())


def apply_rules(df: DataFrame, dataset: str, rules: Sequence[Rule],
                helper_cols: Sequence[str] = ()) -> DQOutcome:
    """Evaluate `rules` on `df`; helper columns are dropped from the outputs."""
    flagged = df
    for r in rules:
        # A NULL predicate means "cannot be evaluated" and must never fail a row by accident.
        flagged = flagged.withColumn(f"_f_{r.rule_id}", F.coalesce(r.fail(), F.lit(False)))
    flagged = flagged.cache()

    aggs = [F.count(F.lit(1)).alias("_total")] + [
        F.sum(F.col(f"_f_{r.rule_id}").cast("int")).alias(r.rule_id) for r in rules]
    row = flagged.agg(*aggs).collect()[0]
    total = int(row["_total"])
    results = [result_record(r, total, int(row[r.rule_id] or 0)) for r in rules]

    critical = [r for r in rules if r.severity == CRITICAL]
    crit_ids = _id_array(critical)
    drop = list(helper_cols) + [f"_f_{r.rule_id}" for r in rules]

    quarantine = (flagged.filter(F.size(crit_ids) > 0)
                  .withColumn("dq_failed_rules", F.concat_ws("|", crit_ids))
                  .withColumn("dq_dataset", F.lit(dataset)).drop(*drop))
    clean = flagged.filter(F.size(crit_ids) == 0).drop(*drop)
    quarantined_rows = flagged.filter(F.size(crit_ids) > 0).count()
    return DQOutcome(clean, quarantine, results, total, quarantined_rows)
