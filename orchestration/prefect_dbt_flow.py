from prefect import flow, task
import subprocess
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DBT_DIR = os.path.join(PROJECT_ROOT, "saas_billing_warehouse")

@task(retries=2, retry_delay_seconds=10)
def run_command(cmd):
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        raise Exception(f"Failed command: {cmd}")

@flow(name="saas-billing-pipeline")
def pipeline():

    run_command("make ingest")
    run_command("make dbt-run")
    run_command("make dbt-test")
    run_command("make dbt-snapshot")
    run_command("make dbt-freshness")

if __name__ == "__main__":
    pipeline()