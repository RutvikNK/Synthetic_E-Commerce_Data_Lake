{{ config(materialized='view') }}

SELECT
    event_id,
    user_id,
    -- Convert unix timestamp (float) to a proper TIMESTAMP object
    TIMESTAMP_SECONDS(CAST(timestamp AS INT64)) as event_time,
    event_type,
    city,
    country,
    -- Handle null revenue safely
    COALESCE(revenue, 0) as revenue
FROM
    {{ source('ecommerce', 'raw_events') }}