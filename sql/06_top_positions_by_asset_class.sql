-- 06  Largest positions per asset class
-- Business question: where is the book concentrated? Top 3 absolute market values per asset class,
-- with each position's share of its asset-class gross market value.
WITH ranked AS (
    SELECT
        asset_class,
        instrument_id,
        net_quantity,
        market_value_usd,
        ROW_NUMBER() OVER (PARTITION BY asset_class ORDER BY ABS(market_value_usd) DESC) AS rn,
        SUM(ABS(market_value_usd)) OVER (PARTITION BY asset_class)                       AS class_gross_mv
    FROM positions
)
SELECT
    asset_class,
    rn AS rank_in_class,
    instrument_id,
    net_quantity,
    market_value_usd,
    ROUND(100.0 * ABS(market_value_usd) / class_gross_mv, 2) AS pct_of_class_gross_mv
FROM ranked
WHERE rn <= 3
ORDER BY asset_class, rn
