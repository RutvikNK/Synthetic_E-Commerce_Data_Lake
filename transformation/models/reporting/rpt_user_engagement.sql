{{ config(materialized='table') }}

select
    session_date,
    
    -- 1. Bounce Rate (Percentage of single-event sessions)
    safe_divide(countif(is_bounce), count(*)) as bounce_rate,
    
    -- 2. Time Spent: Buyers vs Non-Buyers
    avg(case when is_buyer then session_duration_seconds end) as avg_time_buyers,
    avg(case when not is_buyer then session_duration_seconds end) as avg_time_browsers,
    
    -- 3. Overall Stickiness
    avg(session_duration_seconds) as avg_time_overall

from {{ ref('fct_user_sessions') }}
group by 1
order by 1 desc