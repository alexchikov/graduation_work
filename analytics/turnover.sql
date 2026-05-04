WITH trades AS (
    SELECT
        'stock' AS asset_type,
        TRADEDATE,
        VALUE,
        NUMTRADES
    FROM moex_gold.fact_stock_daily
    WHERE 1 = 1
        [[AND TRADEDATE >= toDate({{date_from}})]]
        [[AND TRADEDATE <= toDate({{date_to}})]]

    UNION ALL

    SELECT
        'bond' AS asset_type,
        TRADEDATE,
        VALUE,
        NUMTRADES
    FROM moex_gold.fact_bond_daily
    WHERE 1 = 1
        [[AND TRADEDATE >= toDate({{date_from}})]]
        [[AND TRADEDATE <= toDate({{date_to}})]]
)

SELECT
    TRADEDATE AS trade_date,
    asset_type,
    round(sum(ifNull(VALUE, 0)), 2) AS daily_turnover,
    round(sum(ifNull(NUMTRADES, 0)), 0) AS daily_trades
FROM trades
GROUP BY
    trade_date,
    asset_type
ORDER BY
    trade_date,
    asset_type;