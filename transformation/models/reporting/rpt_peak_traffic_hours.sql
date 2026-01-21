{{ config(materialized='table') }}

with hourly_data as (
    -- Extract the hour from the raw timestamp
    select 
        extract(hour from occurred_at) as hour_of_day,
        extract(dayofweek from occurred_at) as day_of_week, -- 1=Sunday, 7=Saturday
        count(*) as total_events
    from {{ ref('stg_page_views') }}
    group by 1, 2
)

select
    case 
        when day_of_week = 1 then 'Sunday'
        when day_of_week = 2 then 'Monday'
        when day_of_week = 3 then 'Tuesday'
        when day_of_week = 4 then 'Wednesday'
        when day_of_week = 5 then 'Thursday'
        when day_of_week = 6 then 'Friday'
        when day_of_week = 7 then 'Saturday'
    end as day_name,
    hour_of_day,
    total_events,
    
    -- Simple rank to find the busiest slot easily
    rank() over (order by total_events desc) as traffic_rank

from hourly_data
order by day_of_week, hour_of_day