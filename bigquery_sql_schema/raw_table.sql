CREATE TABLE `backfill-safe-data-pipeline.analytics.raw_billing_events` (
  event_id STRING,
  customer_id STRING,
  event_type STRING,
  amount FLOAT64,
  event_ts TIMESTAMP,
  updated_at TIMESTAMP,
  metadata JSON,
  
  -- Raw layer metadata
  batch_id STRING,
  ingestion_ts TIMESTAMP,
  source_file STRING  -- optional: track which generation run
)
PARTITION BY DATE(ingestion_ts)
CLUSTER BY batch_id;