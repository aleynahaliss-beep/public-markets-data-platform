-- 05  Margin calls and returns by rating bucket
-- Business question: how much collateral do we need to call (or give back), and which rating
-- buckets drive it? Counterparties without a CSA are listed with their uncollateralised exposure.
SELECT
    COALESCE(rating, 'UNRATED')                                              AS rating,
    COUNT(*)                                                                 AS counterparties,
    SUM(CASE WHEN margin_action = 'MARGIN_CALL'       THEN 1 ELSE 0 END)     AS calls,
    SUM(CASE WHEN margin_action = 'COLLATERAL_RETURN' THEN 1 ELSE 0 END)     AS returns,
    SUM(CASE WHEN margin_action = 'NO_CSA'            THEN 1 ELSE 0 END)     AS no_csa,
    ROUND(SUM(CASE WHEN margin_amount_usd > 0 THEN margin_amount_usd ELSE 0 END), 2)  AS total_call_usd,
    ROUND(SUM(CASE WHEN margin_amount_usd < 0 THEN -margin_amount_usd ELSE 0 END), 2) AS total_return_usd,
    ROUND(SUM(uncollateralised_exposure_usd), 2)                             AS uncollateralised_exposure_usd
FROM margin
GROUP BY COALESCE(rating, 'UNRATED')
ORDER BY uncollateralised_exposure_usd DESC
