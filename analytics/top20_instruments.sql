WITH trades AS (
    SELECT
        'stock' AS asset_type,
        TRADEDATE,
        SECID,
        BOARDID,
        SHORTNAME,
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
        SHORTNAME,
        VALUE,
        VOLUME,
        NUMTRADES
    FROM moex_gold.fact_bond_daily
    WHERE 1 = 1
        [[AND TRADEDATE >= toDate({{date_from}})]]
        [[AND TRADEDATE <= toDate({{date_to}})]]
)

SELECT
    t.asset_type,
    t.SECID AS secid,
    anyLast(coalesce(ds.shortname, t.SHORTNAME, ds.name)) AS instrument_name,
    countDistinct(t.TRADEDATE) AS active_days,
    round(sum(ifNull(t.VALUE, 0)), 2) AS total_turnover,
    round(sum(ifNull(t.VOLUME, 0)), 2) AS total_volume,
    round(sum(ifNull(t.NUMTRADES, 0)), 0) AS total_trades
FROM trades t
LEFT JOIN moex_gold.dim_security ds
    ON t.SECID = ds.secid
GROUP BY
    t.asset_type,
    t.SECID
ORDER BY total_turnover DESC
LIMIT 20;