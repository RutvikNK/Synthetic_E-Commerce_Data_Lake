{{ config(materialized='table') }}

select
    product_name,
    category,
    sum(total_purchases) as units_sold,
    sum(daily_revenue) as total_revenue,
    -- Calculate Average Order Value (AOV) for this product
    safe_divide(sum(daily_revenue), sum(total_purchases)) as avg_price_point
from {{ ref('fct_daily_product_performance') }}
group by 1, 2
order by total_revenue desc