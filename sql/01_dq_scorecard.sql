-- 01  Data-quality scorecard
-- Business question: which rules are failing, how badly, and which rule hurts most in each dataset?
-- Consumers: data owners, data-engineering backlog grooming.
SELECT
    dataset,
    rule_id,
    rule_name,
    dimension,
    severity,
    failed_rows,
    total_rows,
    ROUND(100.0 * fail_rate, 3)                                   AS fail_pct,
    status,
    RANK() OVER (PARTITION BY dataset ORDER BY failed_rows DESC)  AS rank_in_dataset
FROM dq_results
WHERE status <> 'PASS'
ORDER BY dataset, rank_in_dataset, rule_id
