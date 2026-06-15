# Data Quality Strategy

Data quality is a first-class citizen in the platform, integrated at every layer of the Medallion architecture.

## 1. Automated Checks in Pipeline

*   **Schema Enforcement**: Spark jobs fail if the incoming raw data deviates from the expected schema in the Bronze layer.
*   **Null Validation**: Critical columns (e.g., `customer_id`, `total_amount`) are checked in the Silver layer.
*   **Uniqueness**: The Silver layer transformation enforces deduplication using Window functions.

## 2. Quarantine Pattern

Records that fail critical validation are not dropped. Instead, they are routed to a **Quarantine** zone:
*   Path: `data/silver/quarantine/{entity}`
*   Included metadata: `dq_failed=True`, `dq_reason="Missing critical values"`.
*   Operational impact: Alerts are triggered if quarantine volume exceeds 1% of the batch.

## 3. Post-Processing Validation

The Airflow DAG includes a `validate_gold_quality` task that runs SQL-based checks:
*   **Referential Integrity**: Checks for orphaned orders (orders without valid customers).
*   **Financial Reconciliation**: Ensures `total_amount` in `fact_orders` matches the sum of items/payments.
*   **Volume Anomalies**: Checks if the record count is significantly higher/lower than the 7-day average.

## 4. Monitoring DQ Metrics

Quality metrics are logged in JSON format and can be visualized:
*   Total Records Processed.
*   Pass/Fail Ratio.
*   Common failure reasons per source system.
