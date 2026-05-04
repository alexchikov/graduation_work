SELECT
    'fact_stock_daily' AS table_name,
    count() AS rows_count,
    countDistinct(SECID) AS business_keys_count,
    toString(min(TRADEDATE)) AS min_business_date,
    toString(max(TRADEDATE)) AS max_business_date,
    countIf(CLOSE IS NULL) AS null_main_metric_count,
    round(countIf(CLOSE IS NULL) / count() * 100, 2) AS null_main_metric_pct
FROM moex_gold.fact_stock_daily

UNION ALL

SELECT
    'fact_bond_daily' AS table_name,
    count() AS rows_count,
    countDistinct(SECID) AS business_keys_count,
    toString(min(TRADEDATE)) AS min_business_date,
    toString(max(TRADEDATE)) AS max_business_date,
    countIf(CLOSE IS NULL) AS null_main_metric_count,
    round(countIf(CLOSE IS NULL) / count() * 100, 2) AS null_main_metric_pct
FROM moex_gold.fact_bond_daily

UNION ALL

SELECT
    'fact_security_candles' AS table_name,
    count() AS rows_count,
    countDistinct(source_key) AS business_keys_count,
    toString(min(begin)) AS min_business_date,
    toString(max(begin)) AS max_business_date,
    countIf(close IS NULL) AS null_main_metric_count,
    round(countIf(close IS NULL) / count() * 100, 2) AS null_main_metric_pct
FROM moex_gold.fact_security_candles

UNION ALL

SELECT
    'dim_security' AS table_name,
    count() AS rows_count,
    countDistinct(secid) AS business_keys_count,
    '' AS min_business_date,
    '' AS max_business_date,
    countIf(shortname IS NULL) AS null_main_metric_count,
    round(countIf(shortname IS NULL) / count() * 100, 2) AS null_main_metric_pct
FROM moex_gold.dim_security

UNION ALL

SELECT
    'bridge_security_board' AS table_name,
    count() AS rows_count,
    countDistinct(concat(SECID, '_', BOARDID)) AS business_keys_count,
    '' AS min_business_date,
    '' AS max_business_date,
    countIf(SECNAME IS NULL) AS null_main_metric_count,
    round(countIf(SECNAME IS NULL) / count() * 100, 2) AS null_main_metric_pct
FROM moex_gold.bridge_security_board

UNION ALL

SELECT
    'dim_market' AS table_name,
    count() AS rows_count,
    countDistinct(id) AS business_keys_count,
    '' AS min_business_date,
    '' AS max_business_date,
    countIf(market_name IS NULL) AS null_main_metric_count,
    round(countIf(market_name IS NULL) / count() * 100, 2) AS null_main_metric_pct
FROM moex_gold.dim_market;