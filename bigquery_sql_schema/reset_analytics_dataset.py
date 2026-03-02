from google.cloud import bigquery

PROJECT_ID = "backfill-safe-data-pipeline"
DATASET_ID = "analytics"

RAW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw_billing_events"
BRONZE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.bronze_billing_events"

client = bigquery.Client(project=PROJECT_ID)


def create_raw_table():
    schema = [
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("event_type", "STRING"),
        bigquery.SchemaField("amount", "FLOAT64"),
        bigquery.SchemaField("event_ts", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("metadata", "STRING"),
        bigquery.SchemaField("batch_id", "STRING"),
        bigquery.SchemaField("ingestion_ts", "TIMESTAMP"),
    ]

    table = bigquery.Table(RAW_TABLE, schema=schema)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingestion_ts",
    )

    table.clustering_fields = ["batch_id"]

    try:
        client.get_table(RAW_TABLE)
        print("RAW table already exists.")
    except Exception:
        client.create_table(table)
        print("RAW table created.")


def create_bronze_table():
    schema = [
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("event_type", "STRING"),
        bigquery.SchemaField("amount", "FLOAT64"),
        bigquery.SchemaField("event_ts", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("metadata", "STRING"),
        bigquery.SchemaField("ingestion_ts", "TIMESTAMP"),
    ]

    table = bigquery.Table(BRONZE_TABLE, schema=schema)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="event_ts",
    )

    try:
        client.get_table(BRONZE_TABLE)
        print("BRONZE table already exists.")
    except Exception:
        client.create_table(table)
        print("BRONZE table created.")


def reset_dataset():
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    tables = client.list_tables(dataset_ref)

    for table in tables:
        table_id = f"{dataset_ref}.{table.table_id}"
        print(f"Dropping {table_id}")
        client.delete_table(table_id, not_found_ok=True)

    print("All tables deleted successfully.")


if __name__ == "__main__":
    reset_dataset()
    create_raw_table()
    create_bronze_table()