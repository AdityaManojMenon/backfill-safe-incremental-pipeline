from google.cloud import bigquery

PROJECT_ID = "backfill-safe-data-pipeline"
client = bigquery.Client(project=PROJECT_ID)

def run_query(query, label):
    job_config = bigquery.QueryJobConfig(use_query_cache=False)
    job = client.query(query, job_config=job_config)
    job.result()

    bytes_processed = job.total_bytes_processed
    mb = bytes_processed / (1024 * 1024)
    print(f"{label}: {mb:.2f} MB processed")


def benchmark():
    print("\n=== PERFORMANCE BENCHMARK ===\n")

    run_query("""
        SELECT COUNT(*)
        FROM analytics.fct_customer_mrr
    """, "Full table scan")

    run_query("""
        SELECT COUNT(*)
        FROM analytics.fct_customer_mrr
        WHERE billing_month BETWEEN '2026-01-01' AND '2026-02-01'
    """, "Single month filter")

    run_query("""
        SELECT customer_id, SUM(mrr)
        FROM analytics.fct_customer_mrr
        WHERE customer_id = 'customer_000042'
        GROUP BY 1
    """, "Single customer filter")


if __name__ == "__main__":
    benchmark()