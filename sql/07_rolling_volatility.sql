-- 07  Rolling 20-day volatility (latest date, annualised)
-- Business question: which instruments are currently the most volatile relative to their own history?
-- Uses a 20-row window per instrument on the USD return series produced by the risk module.
WITH vol AS (
    SELECT
        instrument_id,
        asset_class,
        price_date,
        STDDEV_SAMP(ret) OVER (PARTITION BY instrument_id ORDER BY price_date
                               ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * SQRT(252) AS vol_20d_ann,
        COUNT(ret)       OVER (PARTITION BY instrument_id ORDER BY price_date
                               ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)             AS obs,
        ROW_NUMBER()     OVER (PARTITION BY instrument_id ORDER BY price_date DESC)   AS rn
    FROM returns_usd
)
SELECT
    instrument_id,
    asset_class,
    price_date                                                  AS as_of_date,
    ROUND(100 * vol_20d_ann, 2)                                 AS vol_20d_ann_pct,
    RANK() OVER (PARTITION BY asset_class ORDER BY vol_20d_ann DESC) AS vol_rank_in_class
FROM vol
WHERE rn = 1 AND obs = 20
ORDER BY vol_20d_ann DESC
LIMIT 15
