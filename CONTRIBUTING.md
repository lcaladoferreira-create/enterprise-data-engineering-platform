# Contributing to the Enterprise Data Platform

This project implements a professional-grade Data Engineering platform.

## Development Process

This project was developed using **Jules**, a senior software engineer AI agent. The following process was used to ensure quality and production readiness:

1.  **Scaffolding**: Jules generated the initial Medallion Architecture and multi-cloud infrastructure based on senior-level engineering requirements.
2.  **Logic Implementation**: Incremental Spark processing (high-watermark), PII masking, and Star Schema generation were implemented with full Python type hints and structured logging.
3.  **Manual Validation**: Every critical module was validated using terminal commands:
    *   `python -m compileall`: Ensuring byte-code consistency across all Python scripts.
    *   `pytest`: Logic verification for Spark transformations and Data Quality utilities.
    *   `ruff check`: Static analysis and linting compliance.
    *   `docker compose config`: Validation of the multi-database local orchestration stack.
    *   `terraform fmt`: Infrastructure code formatting.

## Standards

- **Python**: PEP 8 compliance, multi-line formatting, type hints, and structured JSON logging.
- **Orchestration**: No "dummy" or "simulated" checks; all Airflow tasks perform real data movement or validation.
- **Security**: GDPR/LGPD compliance through hashing and least-privilege Terraform configurations.

## How to Contribute

1.  Create a feature branch.
2.  Implement changes ensuring full test coverage in `tests/`.
3.  Execute the validation suite using `make lint` and `make test`.
4.  Submit a PR for review.
