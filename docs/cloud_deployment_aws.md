# Cloud Deployment: AWS

## Architecture Mapping

*   **Storage**: Amazon S3 (Buckets for raw, bronze, silver, gold).
*   **Compute**: AWS Glue or Amazon EMR (Spark Serverless/Cluster).
*   **Orchestration**: Amazon MWAA (Managed Workflows for Apache Airflow).
*   **Database**: Amazon RDS (PostgreSQL/MySQL), Amazon DocumentDB, Amazon Keyspaces.
*   **Security**: IAM Roles, AWS Secrets Manager, KMS.
*   **Monitoring**: Amazon CloudWatch, AWS CloudTrail.

## Deployment Steps

1.  **Network**: Deploy a VPC with private subnets for databases and compute.
2.  **Storage**: Run the Terraform module in `infrastructure/aws/` to create S3 buckets.
3.  **Secrets**: Store database passwords in Secrets Manager.
4.  **Airflow**: Deploy MWAA and upload the `dags/` folder to the dedicated S3 bucket.
5.  **Spark**: Package the `spark/` application and upload to S3. Configure Glue jobs to point to the main script.

## Security Hardening
*   Enable S3 Bucket Versioning and MFA Delete.
*   Use VPC Endpoints for S3 and Glue to keep traffic within the AWS network.
*   Apply Service Control Policies (SCPs) to restrict unauthorized region usage.
