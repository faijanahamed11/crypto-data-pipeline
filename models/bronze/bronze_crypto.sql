{{
  config(
    materialized = 'table',
    )
}}
SELECT * 
FROM {{ source('crypto_source', 'crypto_data') }}