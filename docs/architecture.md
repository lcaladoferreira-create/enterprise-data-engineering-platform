# Platform Architecture

## Data Layers (Medallion Architecture)

1.  **Raw Layer**:
    - Landing zone for source data.
    - Format: Original source format (mostly JSON).
    - Immutable storage.

2.  **Bronze Layer (Validated)**:
    - Data converted to Parquet for performance.
    - Added metadata: `ingestion_timestamp`, `source_file`.
    - Schema validation applied.

3.  **Silver Layer (Cleaned & Standardized)**:
    - Deduplication using business keys.
    - Standardized formats (dates, phone numbers, emails).
    - Null handling and basic data cleaning.
    - PII masking where appropriate.

4.  **Gold Layer (Curated/Analytical)**:
    - Business-level aggregates.
    - Joined tables (e.g., `customer_orders`).
    - Optimized for BI and reporting.

## Technology Stack

-   **Ingestion**: Apache NiFi handles the movement of data from transactional databases to the Raw layer.
-   **Orchestration**: Apache Airflow manages the workflow, scheduling Spark jobs and running validation tasks.
-   **Processing**: Apache Spark (PySpark) provides the heavy lifting for data transformations across layers.
-   **Storage**:
    - Local: Filesystem (simulating HDFS/S3).
    - Cloud: S3 (AWS), GCS (GCP), ADLS Gen2 (Azure).
-   **Source Databases**: PostgreSQL, MySQL, MongoDB, Cassandra.

## Data Flow Pattern

1.  NiFi extracts data from sources via JDBC/Drivers.
2.  NiFi writes data to `data/raw/{entity}/`.
3.  Airflow DAG triggers.
4.  Spark Job: Raw -> Bronze.
5.  Spark Job: Bronze -> Silver.
6.  Spark Job: Silver -> Gold.
7.  Analytical queries run against Gold layer.
