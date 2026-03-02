from google.cloud import bigquery

PROJECT_ID = "backfill-safe-data-pipeline"
DATASET_ID = "analytics"

client = bigquery.Client(project=PROJECT_ID)

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