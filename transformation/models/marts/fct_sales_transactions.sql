{{ config(materialized='incremental') }}

select
    event_id as transaction_id,
    user_id,
    timestamp as transaction_time,
    product_name,
    price as revenue,
    location
from {{ ref('stg_purchase') }}
{% if is_incremental() %}
  where timestamp > (select max(transaction_time) from {{ this }})
{% endif %}