WITH trades AS (
    SELECT
        'stock' AS asset_type,
        TRADEDATE,
        SECID,
        BOARDID,
        VALUE,
        VOLUME,
        NUMTRADES
    FROM moex_gold.fact_stock_daily
    WHERE 1 = 1
        [[AND TRADEDATE >= toDate({{date_from}})]]
        [[AND TRADEDATE <= toDate({{date_to}})]]

    UNION ALL

    SELECT
        'bond' AS asset_type,
        TRADEDATE,
        SECID,
        BOARDID,
        VALUE,
        VOLUME,
        NUMTRADES
    FROM moex_gold.fact_bond_daily
    WHERE 1 = 1
        [[AND TRADEDATE >= toDate({{date_from}})]]
        [[AND TRADEDATE <= toDate({{date_to}})]]
),

agg AS (
    SELECT
        asset_type,
        count() AS rows_count,
        countDistinct(SECID) AS instruments_count,
        countDistinct(BOARDID) AS boards_count,
        round(sum(ifNull(VALUE, 0)), 2) AS total_turnover,
        round(sum(ifNull(VOLUME, 0)), 2) AS total_volume,
        round(sum(ifNull(NUMTRADES, 0)), 0) AS total_trades,
        min(TRADEDATE) AS min_trade_date,
        max(TRADEDATE) AS max_trade_date
    FROM trades
    GROUP BY asset_type
)

SELECT
    asset_type,
    rows_count,
    instruments_count,
    boards_count,
    total_turnover,
    total_volume,
    total_trades,
    min_trade_date,
    max_trade_date,

    multiIf(
        total_turnover >= 1000000000, 'high turnover',
        total_turnover >= 100000000, 'medium turnover',
        total_turnover > 0, 'low turnover',
        'no turnover'
    ) AS turnover_status,

    multiIf(
        max_trade_date >= today() - 1, 'fresh',
        max_trade_date >= today() - 7, 'warning',
        'stale'
    ) AS freshness_status,

    multiIf(
        total_turnover >= 1000000000 AND max_trade_date >= today() - 1, 3,
        total_turnover >= 100000000 AND max_trade_date >= today() - 7, 2,
        total_turnover > 0, 1,
        0
    ) AS format_score

FROM agg
ORDER BY total_turnover DESC;