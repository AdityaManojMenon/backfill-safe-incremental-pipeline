{{
  config(
    materialized='incremental',
    unique_key=['billing_month', 'customer_id'],
    incremental_strategy='merge',
    partition_by={"field": "billing_month", "data_type": "date"},
    cluster_by=["customer_id"]
  )
}}

WITH base AS(

    SELECT
        DATE_TRUNC(DATE(event_ts), MONTH) AS billing_month,
        customer_id,
        event_type,
        amount,

        CASE
            WHEN event_type = 'invoice_paid' THEN amount
            WHEN event_type = 'invoice_adjusted' THEN amount
            WHEN event_type = 'invoice_refunded' THEN -abs(amount)
        END AS net_amount

    FROM {{ ref('stg_billing_events') }}

    {% if is_incremental() %}
    -- only recompute months that can still change due to late updates
    where date(event_ts) >= date_sub(date_trunc(current_date(), month),interval 6 month)
    {% endif %}
),

monthly_customer_revenue AS(
    SELECT 
        billing_month,
        customer_id,
        SUM(net_amount) AS customer_mrr
    FROM base
    GROUP BY 1,2

)

SELECT
  billing_month,
  customer_id,
  customer_mrr
FROM monthly_customer_revenue
