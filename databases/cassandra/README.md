# Cassandra Database Setup

The `init.cql` script contains the DDL and seed data for the Cassandra source database.

## Local Initialization

Note: The official Cassandra Docker image does not automatically execute scripts in `/docker-entrypoint-initdb.d`.

To initialize the database locally after starting the containers, run:

```bash
docker exec -it cassandra-source cqlsh -f /docker-entrypoint-initdb.d/init.cql
```

## Data Model

- **Keyspace**: `events_ks`
- **Tables**:
    - `application_logs`: Stores service logs.
    - `user_events`: Stores raw user behavior events.
