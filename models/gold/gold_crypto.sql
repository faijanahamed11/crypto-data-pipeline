{{
  config(
    materialized = 'table',
    )
}}
SELECT
    id,
    name,
    symbol,
    current_price,
    market_cap,
    price_category,

    -- Ranking logic
    RANK() OVER (ORDER BY market_cap DESC) AS market_rank

FROM {{ ref('silver_crypto') }}