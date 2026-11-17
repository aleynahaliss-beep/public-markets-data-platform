-- 04  Counterparty exposure concentration
-- Business question: how concentrated is our estimated exposure at default (EAD), and how many
-- counterparties make up 80% of it? Cumulative share uses a running window over EAD rank.
SELECT
    counterparty_id,
    counterparty_name,
    rating,
    csa_flag,
    ead_estimate_usd,
    ROUND(100.0 * ead_estimate_usd / SUM(ead_estimate_usd) OVER (), 2)                         AS share_pct,
    ROUND(100.0 * SUM(ead_estimate_usd) OVER (ORDER BY ead_estimate_usd DESC
                                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                / SUM(ead_estimate_usd) OVER (), 2)                                            AS cumulative_share_pct,
    CASE WHEN SUM(ead_estimate_usd) OVER (ORDER BY ead_estimate_usd DESC
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
              / SUM(ead_estimate_usd) OVER () <= 0.80
         THEN 'top-80%' ELSE 'tail' END                                                        AS bucket
FROM counterparty_exposure
ORDER BY ead_estimate_usd DESC
