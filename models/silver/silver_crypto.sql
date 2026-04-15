{{
  config(
    materialized = 'table',
    )
}}
SELECT
    id,
    symbol,
    name,
    current_price,
    market_cap,

    CASE 
        WHEN current_price > 50000 THEN 'HIGH'
        WHEN current_price > 10000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS price_category,


FROM {{ ref('bronze_crypto') }}