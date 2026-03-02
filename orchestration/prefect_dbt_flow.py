from prefect import flow, task
import subprocess
from data_generator.generate_events import generate_billing_events
from data_generator.generate_updates import generate_updates
from data_generator.upload_to_bigquery import initial_load, incremental_load


@task
def generate_base():
    return generate_billing_events(
        num_customers=50000,
        months_back=24
    )


@task
def generate_update_data(base_df):
    return generate_updates(base_df)


@task
def load_initial(df):
    return initial_load(df)


@task
def load_incremental(df):
    return incremental_load(df)


@task
def run_dbt():
    subprocess.run(["dbt", "build"], check=True)


@flow(name="saas_billing_pipeline")
def main_flow():
    base = generate_base()
    load_initial(base)

    updates = generate_update_data(base)
    load_incremental(updates)

    run_dbt()


if __name__ == "__main__":
    main_flow()