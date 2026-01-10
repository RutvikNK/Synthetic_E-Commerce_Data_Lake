{{ config(materialized='table') }}

SELECT
    DATE(event_time) as sale_date,
    city,
    country,
    COUNT(*) as total_transactions,
    SUM(revenue) as daily_revenue
FROM
    {{ ref('stg_events') }}
WHERE
    event_type = 'purchase'
GROUP BY
    1, 2, 3