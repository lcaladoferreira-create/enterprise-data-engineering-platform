# Local Setup Guide

Follow these steps to run the Data Engineering platform on your local machine for development and testing.

## Prerequisites

*   **Docker Desktop** (at least 8GB RAM assigned)
*   **Python 3.9+**
*   **Make** (optional, but recommended)

## 1. Environment Setup

Clone the repository and copy the example environment file:

```bash
git clone https://github.com/your-org/enterprise-data-platform.git
cd enterprise-data-platform
cp .env.example .env
```

## 2. Start Services

Use Docker Compose to spin up the source databases, Airflow, and Spark:

```bash
make up
# or
docker compose up -d
```

Wait a few minutes for all health checks to pass. You can check the status with `docker compose ps`.

## 3. Initialize Sample Data

Run the seeding script to simulate data ingestion from source systems to the Raw layer:

```bash
make init-data
# or
python3 scripts/seed_data.py
```

## 4. Access the UI

*   **Airflow Webserver**: [http://localhost:8080](http://localhost:8080) (Default: `admin` / `admin`)
*   **Spark Master**: [http://localhost:8081](http://localhost:8081)
*   **PostgreSQL**: `localhost:5432`
*   **MySQL**: `localhost:3306`

## 5. Running Tests

Execute the validation suite:

```bash
make test
# or
pytest tests/
```

## 6. Cleanup

To stop and remove all containers and volumes:

```bash
make clean
```
