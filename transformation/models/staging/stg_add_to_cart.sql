with source as (
    select * from {{ source('ecommerce', 'raw_add_to_cart') }}
),

renamed as (
    select
        event_id,
        session_id,
        user_id,
        product_id,
        product_name,
        category,
        price,
        location,
        device,
        -- Parse the string timestamp into a real BigQuery TIMESTAMP
        TIMESTAMP(timestamp) as occurred_at,
        -- Extract the date for partitioning
        DATE(TIMESTAMP(timestamp)) as date_key
    from source
)

select * from renamed