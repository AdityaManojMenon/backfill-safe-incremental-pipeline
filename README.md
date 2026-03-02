# Backfill-Safe Incremental SaaS Analytics Warehouse  
### BigQuery + dbt + Partitioning + Revenue Bridge Modeling

A production-style SaaS revenue analytics warehouse built with:

- **BigQuery** (partitioned + clustered storage)
- **dbt** (incremental + snapshot models)
- **Python ingestion layer** (append-only raw + merge-based bronze)
- **CI pipeline** (dbt build validation)
- Designed to handle late-arriving updates, backfills, and revenue restatements safely.

---

## Project Overview

This project simulates a SaaS billing system at scale (~800k+ events) and builds a fully incremental analytics warehouse capable of:

- Append-only raw ingestion with batch tracking
- Deduplicated bronze layer with MERGE logic
- Idempotent incremental transformations
- Churn / expansion / contraction revenue modeling
- Partition-aware cost-efficient queries
- CI-validated dbt builds

---

## Architecture

Data Generator
↓
RAW (Append-only, partitioned by ingestion_ts)
↓
BRONZE (MERGE-based deduplicated fact table)
↓
dbt Staging
↓
fct_customer_mrr (Incremental, partitioned)
↓
fct_mrr_bridge (Revenue movement classification)
↓
fct_mrr_kpis (Churn + Net Retention Metrics)

---

## Key Engineering Features

### 1. Append-Only Raw Layer
- Immutable event ingestion
- Batch-level tracking
- Partitioned by ingestion timestamp
- Clustered by batch_id

### 2. Bronze MERGE Logic
- Deduplicates on `event_id`
- Prioritizes latest `updated_at`
- Handles late-arriving updates
- Supports selective reprocessing

### 3. Backfill-Safe Incremental dbt Models
- Unique-key incremental strategy
- Partition-aware rebuild logic
- No full table reload required
- Idempotent retries

### 3. Revenue Movement Modeling
Classifies monthly MRR changes into:
- New MRR
- Churn MRR
- Expansion MRR
- Contraction MRR
- No-change

Revenue Bridge Validation: Start MRR + New + Expansion - Contraction - Churn = End MRR

### 5 Snapshot (SCD Type 2)
- Tracks historical customer state
- Enables longitudinal churn analysis

### 6 Cost-Aware Storage Design
- Partitioned by billing_month
- Clustered by customer_id
- Partition pruning validated at scale

---

## Scaling & Performance Test

Dataset scaled to ~874k base events + updates.

Raw rows: ~1.7M  
Bronze rows: ~874k (deduplicated)

Partition Pruning Test:

| Query Type | Bytes Processed |
|------------|------------------|
| Full table scan | ~12 MB |
| Single month filter | ~0.54 MB |
| Single customer filter | ~0.1 MB |


Result: Significant scan reduction via partitioning + clustering.

---

## Orchestration

Automating the who pipeline using prefect as primary orchestrator and makefile to run the prefect pipeline as a wrapper for convienece. Just call make pipeline to run the pipeline.

## Data Validation

Automated checks confirm:

- No duplicate event_id in bronze
- No stale rows (raw.updated_at > bronze.updated_at)
- Incremental updates applied correctly

dbt tests enforce:
- Not-null constraints
- Unique keys
- Accepted values
- Source freshness

---

## Why This Project Matters

This project demonstrates:
Production-style ingestion design
Backfill-safe incremental modeling
Revenue-grade SaaS analytics
Cost-efficient BigQuery engineering
Scalable warehouse architecture
Designed to reflect real-world SaaS billing systems and analytics engineering workflows.

---

## Tech Stack
1. Python 3.11
2. BigQuery
3. dbt-core 1.7
4. Prefect 


