## Overview

This project implements a backfill-safe incremental analytics pipeline designed to handle late-arriving data, historical corrections, and pipeline retries without requiring full table reloads.

The system focuses on analytics correctness and cost efficiency once data has already landed in the data warehouse. It intentionally does not address real-time ingestion or change data capture. Will extend alot of core features of this project in CDC pipeline project in the future.

## High level Architecture
Source Event Table (BigQuery) -> Bronze Layer (Raw incremental models)
