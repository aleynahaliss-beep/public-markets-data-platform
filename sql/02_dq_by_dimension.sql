-- 02  DQ pass-rate by dimension
-- Business question: across all rules, which data-quality dimensions are weakest?
-- Pass-rate here is rule-level (rules passing / rules evaluated), the usual executive roll-up.
SELECT
    dimension,
    COUNT(*)                                                        AS rules_evaluated,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)                AS rules_passing,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END)                AS rules_failing_critical,
    SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END)                AS rules_warning,
    ROUND(100.0 * SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 1) AS rule_pass_rate_pct,
    SUM(failed_rows)                                                AS failing_records
FROM dq_results
GROUP BY dimension
ORDER BY rule_pass_rate_pct, dimension
