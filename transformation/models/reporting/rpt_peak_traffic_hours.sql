{{ config(materialized='table') }}

with hourly_data as (
    -- Extract the hour from the raw timestamp
    select 
        extract(hour from occurred_at) as hour_of_day,
        extract(dayofweek from occurred_at) as day_of_week, -- 1=Sunday, 7=Saturday
        format_timestamp('%A', occurred_at) as day_name,
        count(*) as total_events
    from {{ ref('stg_page_views') }}
    group by 1, 2, 3
)
select
    day_name,
    hour_of_day,
    total_events,
    
    -- Simple rank to find the busiest slot easily
    rank() over (order by total_events desc) as traffic_rank

from hourly_data
order by day_of_week, hour_of_day