{{ config(
    materialized='incremental',
    partition_by={
      "field": "date_key",
      "data_type": "date",
      "granularity": "day"
    }
) }}

with views as (
    select 
        date_key, 
        location, 
        product_id, 
        product_name,
        category,
        count(*) as total_views
    from {{ ref('stg_page_views') }}
    {% if is_incremental() %}
      where date_key >= (select max(date_key) from {{ this }})
    {% endif %}
    group by 1, 2, 3, 4, 5
),

carts as (
    select 
        date_key, 
        location, 
        product_id, 
        count(*) as total_adds
    from {{ ref('stg_add_to_cart') }}
    {% if is_incremental() %}
      where date_key >= (select max(date_key) from {{ this }})
    {% endif %}
    group by 1, 2, 3
),

purchases as (
    select 
        date_key, 
        location, 
        product_id, 
        count(*) as total_purchases,
        sum(price) as daily_revenue
    from {{ ref('stg_purchase') }}
    {% if is_incremental() %}
      where date_key >= (select max(date_key) from {{ this }})
    {% endif %}
    group by 1, 2, 3
)

select
    v.date_key,
    v.location,
    v.product_id,
    v.product_name,
    v.category,
    
    -- Metrics
    v.total_views,
    coalesce(c.total_adds, 0) as total_adds,
    coalesce(p.total_purchases, 0) as total_purchases,
    coalesce(p.daily_revenue, 0.0) as daily_revenue,

    -- Complex Metrics (Conversion Rates)
    safe_divide(p.total_purchases, v.total_views) as view_to_purchase_rate

from views v
left join carts c 
    on v.date_key = c.date_key 
    and v.location = c.location 
    and v.product_id = c.product_id
left join purchases p 
    on v.date_key = p.date_key 
    and v.location = p.location 
    and v.product_id = p.product_id