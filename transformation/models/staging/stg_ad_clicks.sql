with source as (
    select * from {{ source('ecommerce', 'raw_ad_click') }}
),

renamed as (
    select
        event_id,
        session_id,
        user_id,
        -- Timestamps always need casting from String to Timestamp
        TIMESTAMP(timestamp) as occurred_at,
        DATE(TIMESTAMP(timestamp)) as date_key,
        
        -- Device & Location (Standard fields)
        device,
        location,
        
        -- Ad Specifics (Handle NULLs if necessary)
        coalesce(ad_source, 'unknown') as ad_source,
        coalesce(campaign_id, 'organic') as campaign_id

    from source
)

select * from renamed