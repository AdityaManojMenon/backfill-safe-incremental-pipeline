## Overview

This project implements a backfill-safe incremental analytics pipeline designed to handle late-arriving data, historical corrections, and pipeline retries without requiring full table reloads.

The system focuses on analytics correctness and cost efficiency once data has already landed in the data warehouse. It does not address real-time ingestion or change data capture. CDC project will becoming soon!

## High level Architecture
![alt text](high-level-architecture-diagram.png)

Data flows through a standard Bronze → Silver → Gold modeling pattern.Each layer is implemented using dbt incremental models and is partitioned by event date to support selective reprocessing.

## Source Data Characteristics

The source data simulates a SaaS billing event stream similar to those produced by subscription-based platforms (e.g. Stripe-style billing systems). Events represent lifecycle changes such as subscription creation, plan upgrades, invoice payments, refunds, and cancellations.

Data is generated synthetically using Python to intentionally reproduce real-world billing behaviors, including late-arriving updates and historical corrections.

Each event record includes:
- event_id: stable primary key identifying the billing event
- customer_id: identifier for the subscribing customer
- event_type: billing action (e.g. invoice_paid, refund_issued)
- amount: monetary impact of the event
- event_ts: business event timestamp
- updated_at: last modification timestamp

Late-arriving data is simulated by modifying historical billing events with newer updated_at values days or weeks after the original event_ts, reflecting real-world scenarios such as refunds and dispute resolutions.

## Incremental Processing Strategy

All models are implemented as dbt incremental models using MERGE semantics. Rather than appending only new records, models selectively reprocesstime-based partitions to incorporate late-arriving updates.By default, a rolling reprocessing window is applied to recent partitions.This window can be overridden to perform targeted historical backfills.

## Backfill Mechanism

Backfills are controlled via dbt runtime variables. When a backfill start date is provided, all partitions greater than or equal to that date are
recomputed deterministically.

This allows:
- Recovery from failed runs
- Historical corrections
- Controlled reprocessing without full reloads

## Idempotency & Data Correctness Guarantees

The pipeline is designed to be idempotent:
- Re-running the same job with the same inputs produces the same outputs
- Duplicate records are prevented using stable primary keys
- MERGE operations ensure updates overwrite prior values

Partition-aware rebuilds ensure that downstream aggregates remain consistent
even when historical data changes.

## Failure Scenarios & Recovery

Scenario: Pipeline fails mid-run  
→ Re-running the job safely recomputes affected partitions.

Scenario: Late update modifies a historical record  
→ Backfill reprocesses only impacted partitions.

Scenario: Duplicate source records  
→ Deduplication logic in the Silver layer ensures correctness.

## Orchestration Model
Airflow is used solely for orchestration:
- Scheduling dbt runs
- Triggering manual backfills
- Retrying failed executions

All transformation logic lives in dbt. Airflow does not contain business logic.

## Possible Extensions

- Replace synthetic source data with a real API or OLTP system
- Add upstream CDC ingestion feeding the Bronze layer
- Introduce automated data quality monitoring