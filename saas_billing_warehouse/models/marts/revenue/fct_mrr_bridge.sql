-- Revenue detailed breakdown model
{{
  config(
    materialized='incremental',
    unique_key=['billing_month','customer_id'],
    partition_by={"field": "billing_month", "data_type": "date"},
    cluster_by=["customer_id"],
    incremental_strategy='merge'
  )
}}

WITH m AS (
    SELECT 
        billing_month,
        customer_id,
        mrr
    FROM {{ ref('fct_customer_mrr') }}
),

prev AS (
    SELECT 
      billing_month,
      customer_id,
      mrr,
      LAG(mrr) OVER (PARTITION BY customer_id ORDER BY billing_month) AS prev_mrr,
      LAG(billing_month) OVER (PARTITION BY customer_id ORDER BY billing_month) AS prev_month
    FROM m
),

classified AS (
  SELECT
    billing_month,
    customer_id,
    mrr,
    COALESCE(prev_mrr, 0) as prev_mrr,

    -- core movements
    CASE
      WHEN COALESCE(prev_mrr, 0) = 0 and mrr > 0 THEN 'new_or_reactivated'
      WHEN prev_mrr > 0 and mrr = 0 THEN 'churn'
      WHEN prev_mrr > 0 and mrr > prev_mrr THEN 'expansion'
      WHEN prev_mrr > 0 and mrr < prev_mrr THEN 'contraction'
      WHEN prev_mrr = mrr THEN 'no_change'
      ELSE 'other'
    END AS movement,

    -- amounts
    CASE
      WHEN COALESCE(prev_mrr, 0) = 0 and mrr > 0 THEN mrr
      ELSE 0
    END AS new_mrr,

    CASE 
      WHEN prev_mrr > 0 AND mrr = 0 THEN prev_mrr
      ELSE 0
    END AS churn_mrr,
      
    CASE
      WHEN prev_mrr > 0 AND mrr > prev_mrr THEN (mrr - prev_mrr)
      ELSE 0
    END AS expansion_mrr,

    CASE
      WHEN prev_mrr > 0 AND mrr < prev_mrr THEN (prev_mrr - mrr)
      ELSE 0
    END AS contraction_mrr
  FROM prev
)

SELECT * FROM classified

{% if is_incremental() %}
WHERE billing_month >= DATE_SUB(
  (SELECT MAX(billing_month) FROM {{ this }}),
  INTERVAL 2 MONTH
)
{% endif %}