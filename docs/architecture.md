# Medallion Architecture

The platform follows the Medallion Architecture to ensure data quality and reliability as it flows through the pipeline.

## 1. Raw Layer (Opaque)
*   **Format**: JSON/Avro (original source format).
*   **Storage**: `data/raw/{entity}`.
*   **Nature**: Immutable landing zone. Data is organized by ingestion date.

## 2. Bronze Layer (Validated)
*   **Logic**: Schema enforcement, audit metadata addition, and change tracking hash.
*   **Format**: Parquet.
*   **Metadata**: `batch_id`, `source_system`, `processed_at`, `record_hash`.
*   **Partitioning**: `ingestion_date`.

## 3. Silver Layer (Cleaned)
*   **Logic**: Deduplication based on Primary Key, data standardization, and PII masking.
*   **Deduplication**: Uses Window functions to keep the latest record per PK.
*   **Quarantine**: Records failing critical null checks are moved to a quarantine directory for investigation.
*   **Anonymization**: Sensitive fields (Email, Phone) are hashed using SHA-256.

## 4. Gold Layer (Curated)
*   **Model**: Star Schema.
*   **Dimensions**: `dim_customers`, `dim_products`.
*   **Facts**: `fact_orders`.
*   **Data Marts**: `mart_sales_daily`.
*   **Usage**: Optimized for BI tools (Tableau, PowerBI) and data science notebooks.
