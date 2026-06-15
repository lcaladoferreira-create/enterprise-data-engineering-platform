# Production Ingestion Strategy with Apache NiFi

This document outlines the professional ingestion strategy from diverse sources to the Raw layer.

## Source Systems & Ingestion Modes

| Source | Technology | Ingestion Mode | NiFi Processor |
|--------|------------|----------------|----------------|
| Transactional | PostgreSQL | Incremental (CDC/Timestamp) | `QueryDatabaseTable` |
| Billing | MySQL | Batch (Daily) | `ExecuteSQL` |
| Product Catalog | MongoDB | Full Refresh (Small) | `GetMongo` |
| Logs/Events | Cassandra | Real-time Stream | `QueryCassandra` |

## NiFi Flow Best Practices

1.  **Backpressure & Flow Control**:
    *   Configured on all connections to prevent overloading the Raw storage.
    *   Object Threshold: 10,000; Data Size Threshold: 1 GB.

2.  **Error Handling & Retries**:
    *   Use of `RetryAttribute` and dedicated failure queues.
    *   Failed flowfiles are routed to a `LogAttribute` processor and then to a `Quarantine` bucket.

3.  **Security**:
    *   JDBC connections use **Sensitive Parameter Contexts**.
    *   Data is encrypted in transit using TLS.

4.  **Raw Layer Formatting**:
    *   Data is saved in `data/raw/{entity_name}/{year}/{month}/{day}/{timestamp}.json`.
    *   Allows for efficient discovery and processing by Spark.

## Local Simulation

The `scripts/seed_data.py` script mimics this behavior by generating JSON files in the expected Raw directory structure, which then triggers the Airflow `FileSensor`.

## Production Hardening

In a cloud environment, NiFi should be deployed in a cluster for high availability, using S3/GCS/ADLS processors instead of local `PutFile`.
