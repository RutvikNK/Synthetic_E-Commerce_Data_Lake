{{ config(
    materialized='incremental',
    partition_by={
      "field": "session_date",
      "data_type": "date",
      "granularity": "day"
    }
) }}

with all_events as (
    -- 1. Combine all user actions into a single stream
    select user_id, date_key, occurred_at, 'page_view' as event_type, 0.0 as revenue 
    from {{ ref('stg_page_views') }}

    union all

    select user_id, date_key, occurred_at, 'add_to_cart' as event_type, 0.0 as revenue 
    from {{ ref('stg_add_to_cart') }}

    union all

    select user_id, date_key, occurred_at, 'purchase' as event_type, price as revenue 
    from {{ ref('stg_purchase') }}
    
    union all

    select user_id, date_key, occurred_at, 'ad_click' as event_type, 0.0 as revenue 
    from {{ ref('stg_ad_clicks') }}
)

select
    -- Grain: One row per User per Day
    date_key as session_date,
    user_id,

    -- Session Timing
    min(occurred_at) as session_start_at,
    max(occurred_at) as session_end_at,
    timestamp_diff(max(occurred_at), min(occurred_at), second) as session_duration_seconds,

    -- Activity Counts (The "Engagement" metrics)
    countif(event_type = 'page_view') as total_page_views,
    countif(event_type = 'add_to_cart') as total_cart_adds,
    countif(event_type = 'purchase') as total_purchases,
    countif(event_type = 'ad_click') as total_ad_clicks,

    -- Financials
    sum(revenue) as total_session_revenue,

    -- Flags (Useful for filtering segments easily)
    logical_or(event_type = 'purchase') as is_buyer,
    logical_or(event_type = 'ad_click') as is_ad_traffic,
    
    -- Bounce Logic: If they only had 1 event and it wasn't a purchase
    (count(*) = 1 and countif(event_type = 'purchase') = 0) as is_bounce

from all_events

{% if is_incremental() %}
  where date_key >= (select max(session_date) from {{ this }})
{% endif %}

group by 1, 2