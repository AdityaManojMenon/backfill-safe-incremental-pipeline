-- KPI Table

{{
  config(
    materialized='table',
    partition_by={"field": "billing_month", "data_type": "date"}
  )
}}

WITH b AS (
  SELECT
    billing_month,
    SUM(mrr) AS end_mrr,
    SUM(prev_mrr) AS start_mrr,
    SUM(new_mrr) AS new_mrr,
    SUM(churn_mrr) AS churn_mrr,
    SUM(expansion_mrr) AS expansion_mrr,
    SUM(contraction_mrr) AS contraction_mrr
  FROM {{ ref('fct_mrr_bridge') }}
  GROUP BY 1
)

SELECT
  billing_month,
  start_mrr,
  end_mrr,
  new_mrr,
  churn_mrr,
  expansion_mrr,
  contraction_mrr,
  (start_mrr + new_mrr + expansion_mrr - contraction_mrr - churn_mrr) AS bridge_check,
  safe_divide(churn_mrr, nullif(start_mrr, 0)) AS gross_mrr_churn_rate,
  safe_divide((churn_mrr - expansion_mrr), nullif(start_mrr, 0)) AS net_mrr_churn_rate
FROM b