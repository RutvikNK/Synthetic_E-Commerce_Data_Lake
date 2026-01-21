{{ config(materialized='table') }}

with user_aggregates as (
    select
        user_id,
        min(session_date) as first_seen_date,
        max(session_date) as last_seen_date,
        count(distinct session_date) as total_visit_days,
        sum(total_purchases) as lifetime_orders,
        sum(total_session_revenue) as lifetime_revenue
    from {{ ref('fct_user_sessions') }}
    where is_buyer = true -- We only care about people who spent money
    group by 1
)

select
    user_id,
    first_seen_date,
    lifetime_orders,
    lifetime_revenue,
    
    -- Calculate Average Order Value for this user
    safe_divide(lifetime_revenue, lifetime_orders) as avg_order_value,

    -- Customer Segmentation Logic
    case 
        when lifetime_revenue > 500 then 'VIP'
        when lifetime_revenue > 100 then 'Regular'
        else 'One-Time' 
    end as customer_segment

from user_aggregates
order by lifetime_revenue desc
limit 1000 -- Keeping it manageable for reporting