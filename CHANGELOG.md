# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-06-16

### Added
- **Multi-Cloud IaC**: Production-ready Terraform modules for AWS (S3/Glue), GCP (GCS/Dataproc), and Azure (ADLS/Databricks).
- **Medallion Architecture**: Complete data pipeline implementation (Raw → Bronze → Silver → Gold) using PySpark.
- **Orchestration**: Production-grade Airflow DAGs with real sensors, retries, and data integrity checks.
- **Ingestion**: Scalable multi-source ingestion framework using Apache NiFi for Postgres, MySQL, MongoDB, and Cassandra.
- **Data Quality**: Automated DQ framework with a quarantine pattern for isolating failed records.
- **Security & Compliance**: Integrated PII masking (SHA-256) and exhaustive LGPD/GDPR documentation.
- **Testing**: Comprehensive test suite including unit tests and Spark integration tests for all transformation layers.
- **CI/CD**: GitHub Actions pipeline for automated linting, testing, and container validation.
- **Local Dev**: Docker Compose environment for local stack execution.
