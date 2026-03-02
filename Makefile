# Paths 
DBT_DIR=saas_billing_warehouse
PREFECT_FLOW=orchestration/prefect_flow.py

INGEST_ENV=ingestion_env/bin/activate
DBT_ENV=dbt_env/bin/activate
ORCH_ENV=orchestration_env/bin/activate

# Prefect is the real pipeline
pipeline:
	source $(ORCH_ENV) && \
	python3 $(PREFECT_FLOW)

# dbt standalone (optional)
dbt-run:
	source $(DBT_ENV) && \
	cd $(DBT_DIR) && \
	dbt build