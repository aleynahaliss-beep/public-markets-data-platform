-- 03  Reconciliation breaks by counterparty
-- Business question: which counterparties generate the most front-/back-office breaks, and what is
-- the monetary size of the amount breaks? Trades quarantined by the DQ gate are shown separately
-- because they are a data problem, not a settlement problem.
SELECT
    COALESCE(counterparty_id, 'N/A (BO only)')                                         AS counterparty_id,
    COUNT(*)                                                                           AS breaks,
    SUM(CASE WHEN break_category = 'MISSING_IN_BACK_OFFICE' THEN 1 ELSE 0 END)         AS missing_in_bo,
    SUM(CASE WHEN break_category = 'QUANTITY_BREAK'         THEN 1 ELSE 0 END)         AS quantity_breaks,
    SUM(CASE WHEN break_category = 'AMOUNT_BREAK'           THEN 1 ELSE 0 END)         AS amount_breaks,
    SUM(CASE WHEN break_category = 'SETTLE_DATE_BREAK'      THEN 1 ELSE 0 END)         AS settle_date_breaks,
    ROUND(SUM(CASE WHEN break_category IN ('AMOUNT_BREAK', 'QUANTITY_BREAK')
                   THEN ABS(amount_diff) ELSE 0 END), 2)                               AS abs_value_difference,
    DENSE_RANK() OVER (ORDER BY COUNT(*) DESC)                                         AS break_rank
FROM recon
WHERE recon_status = 'BREAK'
  AND break_category NOT IN ('FO_REJECTED_BY_DQ', 'MISSING_IN_FRONT_OFFICE')
GROUP BY COALESCE(counterparty_id, 'N/A (BO only)')
ORDER BY breaks DESC, counterparty_id
