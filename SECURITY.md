# Security Policy

## Compliance Overview
This platform is designed with "Security by Design" principles to support GDPR and LGPD compliance. For detailed information on PII identification and retention policies, please refer to:
[Security & Compliance Documentation](docs/security_compliance.md)

## Secret Management
- **Local Development**: All secrets are managed via `.env` files which are explicitly excluded from version control via `.gitignore`.
- **Production (Cloud)**: Sensitive credentials (database passwords, API keys) must be stored and retrieved using native cloud secret managers:
  - **AWS**: AWS Secrets Manager
  - **GCP**: Secret Manager
  - **Azure**: Azure Key Vault

## Identity & Access Management (IAM)
The platform follows the Principle of Least Privilege. Required minimum roles:

### AWS
- `S3FullAccess` (Restricted to specific data lake buckets)
- `AWSGlueServiceRole` (For Spark job execution)
- `SecretsManagerReadWrite` (For ingestion credential retrieval)

### GCP
- `roles/storage.objectAdmin` (Restricted to datalake buckets)
- `roles/dataproc.worker` (For Spark processing)
- `roles/secretmanager.secretAccessor` (For ingestion)

### Azure
- `Storage Blob Data Contributor` (For ADLS Gen2)
- `Azure Databricks Workspace Admin`
- `Key Vault Secrets User`

## Reporting a Vulnerability
If you discover a security vulnerability within this project, please report it privately.
Contact: security-disclosure@example.com (Placeholder)

We aim to respond to all reports within 48 hours.
