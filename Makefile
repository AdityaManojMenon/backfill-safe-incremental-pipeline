# Paths 
DBT_DIR=saas_billing_warehouse
PREFECT_FLOW=orchestration/prefect_flow.py

# Envs
INGEST_ENV=ingestoin_env/bin/activate
DBT_ENV=dbt_env/bin/activate
ORCH_ENV=orchestration_env/bin/activate

# Ingestion
ingest:
	source $(INGEST_ENV) && \
	python3 ingestion/test_upload_to_bigquery.py

# dbt
dbt-run:
	source $(DBT_ENV) && \
	cd $(DBT_DIR) && \
	dbt run 

dbt-test:
	source $(DBT_ENV) && \
	cd $(DBT_DIR) && \
	dbt test

dbt-snapshot:
	source $(DBT_ENV) && \
	cd $(DBT_DIR) && \
	dbt snapshot

dbt-freshness:
	source $(DBT_ENV) && \
	cd $(DBT_DIR) && \
	dbt source freshness

# Prefect
prefect-run:
	source $(ORCH_ENV) && \
		python3 $(PREFECT_FLOW)

# Full Pipeline
full:
	make ingest
	make dbt-run
	make dbt-test
	make dbt-snapshot
	make dbt-freshness
