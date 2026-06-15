# Apache NiFi Ingestion Layer

NiFi is used as the primary ingestion tool to move data from source systems (PostgreSQL, MySQL, MongoDB, Cassandra) into the raw data lake layer (`data/raw`).

## Flow Design Overview

The NiFi flow is organized into Process Groups for each source system:

1.  **PostgreSQL Ingestion**:
    *   `QueryDatabaseTable`: Periodically fetches new records from `customers`, `orders`, and `order_items`.
    *   `ConvertAvroToJSON`: Transforms the database format to JSON.
    *   `PutFile`: Saves JSON files to `data/raw/customers`, `data/raw/orders`, etc.

2.  **MySQL Ingestion**:
    *   Similar to PostgreSQL, using `QueryDatabaseTable` for `invoices` and `payments`.

3.  **MongoDB Ingestion**:
    *   `GetMongo`: Queries the `products` and `user_activity` collections.
    *   `PutFile`: Saves to `data/raw/products` and `data/raw/user_activity`.

4.  **Cassandra Ingestion**:
    *   `QueryCassandra`: Fetches `application_logs` and `user_events`.
    *   `PutFile`: Saves to `data/raw/logs`.

## Configuration

The `docker-compose.yml` file mounts the NiFi configuration and the data raw directory. In a production environment, NiFi would use Site-to-Site or S3 processors to move data to a real Data Lake.

## Template Placeholder

A sample NiFi template XML would normally be placed in `nifi/templates/data_ingestion_flow.xml`. For this portfolio, we document the flow logic.

## Security

*   NiFi is configured with HTTPS.
*   Database connections use sensitive parameter contexts for passwords.
*   Data provenance is enabled for full auditability.
