{{ config(
    materialized='incremental',
    partition_by={
      "field": "click_date",
      "data_type": "date",
      "granularity": "day"
    }
) }}

select
    date_key as click_date, 
    ad_source,
    campaign_id,
    location,
    count(distinct event_id) as total_clicks,
    count(distinct user_id) as unique_users_reached

from {{ ref('stg_ad_clicks') }}

{% if is_incremental() %}
  where date_key >= (select max(click_date) from {{ this }})
{% endif %}

group by 1, 2, 3, 4