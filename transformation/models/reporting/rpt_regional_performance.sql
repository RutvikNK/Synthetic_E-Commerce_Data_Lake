{{ config(materialized='table') }}

select
    location,
    sum(total_views) as traffic,
    sum(daily_revenue) as total_revenue,
    -- Calculate Revenue Per Session (a key proxy for quality)
    safe_divide(sum(daily_revenue), sum(total_views)) as revenue_per_view
from {{ ref('fct_daily_product_performance') }}
group by 1
order by total_revenue desc