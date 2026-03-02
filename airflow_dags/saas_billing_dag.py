from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="saas_billing_dbt_showcase",
    start_date=datetime(2026, 2, 25),
    schedule="@daily",
    catchup=False,
) as dag:

    run_full = BashOperator(
        task_id="run_full_pipeline",
        bash_command="cd /path/to/backfill-safe-incremental-pipeline && make full"
    )