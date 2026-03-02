from google.cloud import bigquery
from data_generator.generate_events import generate_billing_events
from data_generator.generate_updates import generate_updates
from data_generator.upload_to_bigquery import initial_load, incremental_load

PROJECT_ID = "backfill-safe-data-pipeline"
DATASET = "analytics"

client = bigquery.Client(project=PROJECT_ID)

def run_validation():

    print("\n=== VALIDATING MERGE LOGIC ===")

    # 1. Check duplicates
    dup_query = f"""
    SELECT COUNT(*) AS duplicate_count
    FROM (
        SELECT event_id
        FROM `{PROJECT_ID}.{DATASET}.bronze_billing_events`
        GROUP BY event_id
        HAVING COUNT(*) > 1
    )
    """

    dup_result = list(client.query(dup_query).result())[0][0]

    print(f"Duplicate event_ids in bronze: {dup_result}")

    # 2. Check bronze is up to date
    freshness_query = f"""
    SELECT COUNT(*) AS stale_rows
    FROM `{PROJECT_ID}.{DATASET}.raw_billing_events` r
    JOIN `{PROJECT_ID}.{DATASET}.bronze_billing_events` b
    ON r.event_id = b.event_id
    WHERE r.updated_at > b.updated_at
    """

    freshness_result = list(client.query(freshness_query).result())[0][0]

    print(f"Stale bronze rows: {freshness_result}")

    # 3. Row counts
    count_query = f"""
    SELECT
        (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET}.raw_billing_events`) AS raw_count,
        (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET}.bronze_billing_events`) AS bronze_count
    """

    counts = list(client.query(count_query).result())[0]

    print(f"Raw rows: {counts[0]}")
    print(f"Bronze rows: {counts[1]}")

    if dup_result == 0 and freshness_result == 0:
        print("\nMerge and dedup logic validated successfully.")
    else:
        print("\nMerge validation failed.")



def run_large_generation():

    print("Generating base 1M+ billing events...")

    base_df = generate_billing_events(
        num_customers=50000,
        months_back=24
    )

    print(f"Generated base rows: {len(base_df)}")

    print("Loading initial base dataset...")
    initial_load(base_df)

    print("Generating updates and refunds...")
    updates_df = generate_updates(base_df)

    print(f"Generated update rows: {len(updates_df)}")

    print("Loading incremental updates...")
    incremental_load(updates_df)

    print("Large dataset generation complete.")


if __name__ == "__main__":
    #run_large_generation()
    run_validation()