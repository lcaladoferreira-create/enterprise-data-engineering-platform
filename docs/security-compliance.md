# Security and Compliance Guide

This document covers the security architecture and compliance (GDPR/LGPD) strategies implemented in the platform.

## Data Security

### Encryption
- **At Rest**:
    - Local: Disk encryption (if enabled by OS).
    - Cloud: S3 SSE-KMS, Azure Storage Encryption, GCS Managed Keys.
- **In Transit**:
    - All database connections use TLS/SSL.
    - NiFi, Airflow, and Spark UI are served over HTTPS.

### Secret Management
- Database credentials and API keys are never hardcoded.
- Local: `.env` file (excluded from Git).
- Cloud: AWS Secrets Manager, GCP Secret Manager, Azure Key Vault.

### Identity and Access Management (IAM)
- Least privilege principles applied to all service accounts.
- Separate roles for Data Engineers, Data Scientists, and Business Analysts.

## Compliance (GDPR & LGPD)

### PII Identification
The following fields are identified as PII (Personally Identifiable Information):
- `customers.first_name`
- `customers.last_name`
- `customers.email`
- `customers.phone`
- `customers.address`

### Data Masking & Anonymization
- In the Silver layer, PII is masked or anonymized for non-production environments.
- Example Spark transformation:
  ```python
  df = df.withColumn("email", F.concat(F.substring(F.col("email"), 1, 2), F.lit("****"), F.substring(F.col("email"), -5, 5)))
  ```

### Right to be Forgotten (Deletion)
- Implementation: A dedicated Airflow DAG handles deletion requests by filtering out specific `customer_id` values from the Gold and Silver layers during the next processing cycle or via a targeted delete job.
- Audit logs capture deletion events for compliance proof.

### Data Retention Policy
- Raw data: 7 years.
- Bronze/Silver: 3 years.
- Gold: 5 years.
- Deletion is automated via lifecycle policies (S3 Lifecycle, GCS Lifecycle).

## Audit Logging
- NiFi Data Provenance: Tracks every change to a flowfile.
- Airflow Task Logs: Tracks pipeline execution.
- CloudTrail / Cloud Logging: Tracks infrastructure changes and access.
