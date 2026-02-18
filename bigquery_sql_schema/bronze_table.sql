CREATE TABLE analytics.bronze_billing_events (
  event_id STRING,
  customer_id STRING,
  event_type STRING,
  amount FLOAT64,
  event_ts TIMESTAMP,
  updated_at TIMESTAMP,
  metadata JSON,
  ingestion_ts TIMESTAMP
)
PARTITION BY DATE(event_ts);