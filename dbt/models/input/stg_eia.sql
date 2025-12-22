{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT 
    REGION,
    DATE,
    SERIES,
    VALUE
FROM {{ source('staging', 'RTO_REGION_HOURLY_STAGING') }}