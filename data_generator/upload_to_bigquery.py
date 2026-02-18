from datetime import datetime, timezone
from typing import Optional
import pandas as pd
from google.cloud import bigquery
import uuid

PROJECT_ID = "backfill-safe-data-pipeline"
DATASET = "analytics"
RAW_TABLE = f"{PROJECT_ID}.{DATASET}.raw_billing_events"
BRONZE_TABLE = f"{PROJECT_ID}.{DATASET}.bronze_billing_events"

# Utilities
def make_batch_id() -> str:
    """Generate unique batch identifier"""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"batch_{ts}_{uuid.uuid4().hex[:8]}"

# Raw Layer
def upload_to_raw(df: pd.DataFrame,batch_id: Optional[str] = None) -> str:
    """
    Append-only load to raw layer (immutable).
    
    Args:
        df: DataFrame with billing events
        batch_id: Unique batch identifier (auto-generated if None)
    
    Returns:
        batch_id for downstream processing
    """
    if batch_id is None:
        batch_id = make_batch_id()
    
    client = bigquery.Client(project=PROJECT_ID)

    # Add raw layer metadata
    df = df.copy()
    df["batch_id"] = batch_id
    df["ingestion_ts"] = datetime.now(timezone.utc)
    
    # Append to raw (idempotent if batch_id is deterministic)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, RAW_TABLE, job_config=job_config)
    job.result()
    
    print(f"Raw: Loaded {len(df)} events (batch_id={batch_id})")
    return batch_id

# Bronze Merge
def merge_raw_to_bronze(batch_id: Optional[str] = None,
                        ingestion_date_from: Optional[str] = None) -> int:
    """
    MERGE raw layer into Bronze fact table.
    Handles:
    1. Late-arriving updates
    2. Deduplication
    3. Backfills
    """
    client = bigquery.Client(project=PROJECT_ID)
    
    where_parts = []
    if batch_id:
        where_parts.append(f"batch_id = '{batch_id}'")
    if ingestion_date_from:
        where_parts.append(f"DATE(ingestion_ts) >= '{ingestion_date_from}'")
    
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    merge_sql = f"""
    MERGE INTO `{BRONZE_TABLE}` T
    USING (
        SELECT * FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY updated_at DESC, ingestion_ts DESC
                ) AS rn
            FROM `{RAW_TABLE}`
            {where_sql}
        )
        WHERE rn = 1
    ) S
    ON T.event_id = S.event_id

    -- Event exists in Bronze AND source has newer data
    WHEN MATCHED AND S.updated_at > T.updated_at THEN
        UPDATE SET
            customer_id = S.customer_id,
            event_type = S.event_type,
            amount = S.amount,
            event_ts = S.event_ts,
            updated_at = S.updated_at,
            metadata = S.metadata,
            ingestion_ts = S.ingestion_ts

    -- Event doesn't exist in Bronze
    WHEN NOT MATCHED THEN
        INSERT (event_id, customer_id, event_type, amount,
                event_ts, updated_at, metadata, ingestion_ts)
        VALUES (S.event_id, S.customer_id, S.event_type, S.amount,
                S.event_ts, S.updated_at, S.metadata, S.ingestion_ts)
    """
    result = client.query(merge_sql).result()
    rows_affected = result.num_dml_affected_rows or 0
    print(f"BRONZE: {rows_affected} rows affected")
    return rows_affected

#Monitoring 
def get_stats() -> pd.DataFrame:
    """Get pipeline stats"""
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    SELECT 
        'raw' AS layer,
        COUNT(*) AS total_events,
        COUNT(DISTINCT batch_id) AS num_batches
    FROM 
        `{RAW_TABLE}`

    UNION ALL

    SELECT 
        'bronze' AS layer,
        COUNT(*) AS total_events,
        NULL AS num_batches

    FROM
        `{BRONZE_TABLE}`
    """
    df = client.query(query).to_dataframe()
    return df
    
# Orchestration Workflows
def initial_load(df: pd.DataFrame) -> str:
    """Full initial reload"""
    print("\n===Initial Reload===")
    batch_id = upload_to_raw(df)
    merge_raw_to_bronze(batch_id=batch_id)
    get_stats()
    return batch_id

def incremental_load(df: pd.DataFrame, batch_id: Optional[str] = None) -> str:
    """Daily Incremental Load"""
    print("\n=== INCREMENTAL LOAD ===")
    batch_id = upload_to_raw(df, batch_id=batch_id)
    merge_raw_to_bronze(batch_id=batch_id)
    get_stats()
    return batch_id

def backfill(df: pd.DataFrame, backfill_date: str) -> str:
    """Backfill historical data."""
    print(f"\n=== BACKFILL: {backfill_date} ===")
    batch_id = f"backfill_{backfill_date.replace('-', '')}_{uuid.uuid4().hex[:8]}"
    upload_to_raw(df, batch_id=batch_id) 
    merge_raw_to_bronze(batch_id=batch_id)
    get_stats()
    return batch_id

def reprocess_bronze(event_date_from: str) -> None:
    """Rebuild Bronze from existing raw data."""
    client = bigquery.Client(project=PROJECT_ID)
    
    print(f"\n=== REPROCESS FROM {event_date_from} ===")
    delete_sql = f"""
    DELETE FROM `{BRONZE_TABLE}`
    WHERE DATE(event_ts) >= '{event_date_from}'
    """
    delete_job = client.query(delete_sql)
    delete_job.result()
    print(f"Deleted Bronze events from {event_date_from}")
    merge_raw_to_bronze(ingestion_date_from=event_date_from)
    print("Reprocessing complete\n")
    get_stats()
    


