-- “customer-month MRR” model
{{
  config(
    materialized='incremental',
    unique_key=['billing_month','customer_id'],
    partition_by={"field": "billing_month", "data_type": "date"},
    cluster_by=["customer_id"],
    incremental_strategy='merge'
  )
}}

with base as (
  SELECT
    DATE_TRUNC(DATE(event_ts), MONTH) as billing_month,
    customer_id,
    event_type,
    amount,
    CASE
      WHEN event_type in ('invoice_paid','invoice_adjusted') THEN amount
      WHEN event_type = 'invoice_refunded' THEN -abs(amount)
      ELSE 0
    END as net_amount
  FROM {{ ref('stg_billing_events') }}
),

monthly_customer as (
  SELECT
    billing_month,
    customer_id,
    sum(net_amount) as mrr
  FROM base
  GROUP BY 1,2
)

SELECT
    billing_month,
    customer_id,
    CAST(mrr AS NUMERIC) AS mrr
FROM monthly_customer

{% if is_incremental() %}
WHERE billing_month >= DATE_SUB(
    (SELECT MAX(billing_month) FROM {{ this }}),
    INTERVAL 2 MONTH
)
{% endif %}


