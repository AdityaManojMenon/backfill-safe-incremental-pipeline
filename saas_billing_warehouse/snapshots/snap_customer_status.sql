{% snapshot snap_customer_status %}

{{
    config(
        target_schema = 'analytics',
        unique_key = 'customer_id',
        strategy = 'timestamp',
        updated_at = 'updated_at'
    )
}}

SELECT
    customer_id,
    MAX(updated_at) AS updated_at,
    COUNT(*) AS total_events
FROM {{ ref('stg_billing_events') }}
GROUP BY 1

{% endsnapshot %}