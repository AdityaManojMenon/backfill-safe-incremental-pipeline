## Responsibility-Driven Directory Structure

This project follows the principle that each top level directory should represent a single responsibility that is an essential part of the overall solution.

Once the problem statement was clearly defined (see README.md),the system was decomposed into four core responsibilities. Each responsibility maps directly to a dedicated directory in the repository.

### Overview of Responsibilities

1. **Data Creation / Ingestion (`data_generator/`)**  
   Goal: Intentionally create the core problem by simulating late-arriving data and historical updates that commonly break incremental pipelines.

2. **Transformation & Correctness (`dbt/`)**  
   Goal: Ensure analytical correctness under retries, backfills, and late updates using idempotent, partition-aware dbt models.

3. **Orchestration & Reliability (`airflow/`)**  
   Goal: Execute transformations safely and repeatedly, enabling scheduled runs and parameterized historical backfills.

4. **Documentation (`docs/`)**  
   Goal: Enable collaboration, onboarding, and extensibility by clearly explaining how the system works and why key design choices were made.


