{{ 
    config(
        materialized='incremental',
        unique_key='event_id',
        partition_by={"field": "event_date", "data_type": "date"},
        cluster_by=["customer_id", "event_type"],
        incremental_strategy="merge"
    ) 
}}

-- incremenral_strategy ensures that if an event_id already exists, it gets updated not duplicated


WITH src AS(

    SELECT
        event_id,
        customer_id,
        event_type,
        amount,
        event_ts,
        updated_at,
        batch_id,
        ingestion_ts,
        metadata,
        date(event_ts) as event_date
    FROM {{ source('analytics', 'raw_billing_events') }}

    {% if is_incremental() %}
    -- Only scan new RAW rows since last successful build (Watermark-based Incremental Load)
    WHERE ingestion_ts > (SELECT ifnull(max(ingestion_ts), timestamp('2000-01-01')) FROM {{ this }})
    {% endif %}
),

ranked AS(
    SELECT 
        *,
        row_number() over(
            PARTITION BY event_id
            ORDER BY updated_at DESC, ingestion_ts DESC
        ) AS rn
    FROM src
)

SELECT
    event_id,
    customer_id,
    event_type,
    amount,
    event_ts,
    updated_at,
    batch_id,
    ingestion_ts,
    metadata,
    event_date
FROM ranked
WHERE rn = 1
