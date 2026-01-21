{{ config(materialized='table') }}

select
    date(timestamp) as ad_date,
    ad_source,     -- e.g., 'facebook', 'google'
    campaign_id,
    count(*) as total_clicks,
    count(distinct user_id) as unique_users_reached
from {{ ref('stg_ad_click') }}  -- You'll need to create this staging model first!
group by 1, 2, 3