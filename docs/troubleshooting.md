# Troubleshooting Guide

## Common Issues and Solutions

### 1. Docker Compose Services Fail to Start
- **Symptom**: `docker-compose up` hangs or containers exit immediately.
- **Solution**:
    - Check for port conflicts (e.g., is another PostgreSQL running on 5432?).
    - Increase Docker memory limit (at least 8GB recommended for the full stack).
    - Run `docker-compose logs <service_name>` to see specific error messages.

### 2. Airflow DAGs Not Appearing
- **Symptom**: Webserver is running but the DAG list is empty.
- **Solution**:
    - Check `airflow-scheduler` logs.
    - Ensure `dags/` folder is correctly mounted.
    - Check for Python syntax errors in the DAG file.

### 3. Spark Jobs Fail with "Class Not Found"
- **Symptom**: `SparkSubmitOperator` fails.
- **Solution**:
    - Ensure all dependencies are included in `--packages` or built into the Docker image.
    - Verify that the Spark Master URL is correct.

### 4. Database Connection Refused
- **Symptom**: NiFi or Spark cannot connect to PostgreSQL/MySQL.
- **Solution**:
    - Verify that the databases are healthy: `docker-compose ps`.
    - Check network connectivity within the Docker network.
    - Ensure the credentials in `.env` match the ones used in database init scripts.

### 5. Data Quality Checks Fail
- **Symptom**: Airflow task `data_quality_checks` fails.
- **Solution**:
    - Query the Silver/Gold tables to find problematic records.
    - Check for schema evolution issues if source data changed.
