# Enterprise Data Quality Standards

This document specifies the Data Quality (DQ) framework integrated into the Medallion architecture.

## 1. Governance Principles
- **Schema First**: Every ingestion task in the Bronze layer enforces a pre-defined Spark StructType.
- **Fail-Fast**: Critical nulls in the Silver layer trigger immediate record isolation.
- **Traceability**: Every record carries a `batch_id` and a `record_hash` (SHA-256) of its raw payload.

## 2. Automated DQ Pattern: The Quarantine
Records failing validation are moved to a specific location for operational investigation:
- **Storage**: `data/silver/quarantine/{entity}/`
- **Metadata**: Fails carry a `dq_failed=True` flag and a `dq_reason` description.

## 3. Business Rule Validations
- **Relational Integrity**: Gold layer Fact tables are validated against Dimensions to ensure no orphaned keys.
- **Financial Reconciliation**: Sum of daily payments is reconciled against daily order totals in the `mart_payment_reconciliation`.
- **Deduplication**: Silver layer applies PK-based deduplication using Window functions to maintain a Type 1 record state.

## 4. Operational Monitoring
The Airflow `verify_pipeline_integrity` task executes final SQL-based validations after the Gold layer is materialized. Alerts are configured for:
- Variance in financial totals.
- Unexpected zero-volume batches.
- High quarantine ratio (>5% of batch).
