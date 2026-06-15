# AWS Deployment Guide

This document outlines the architecture and deployment steps for running the platform on AWS.

## Architecture Mapping

| Local Component | AWS Equivalent |
|-----------------|----------------|
| Raw/Bronze/Silver/Gold | Amazon S3 |
| Apache Spark | AWS Glue / Amazon EMR |
| Apache Airflow | Amazon MWAA (Managed Workflows for Apache Airflow) |
| PostgreSQL/MySQL | Amazon RDS (PostgreSQL/MySQL) |
| MongoDB | Amazon DocumentDB |
| Cassandra | Amazon Keyspaces |
| Secrets | AWS Secrets Manager |
| Security | IAM, KMS |
| Logging | CloudWatch |

## Infrastructure Diagram

(Imagine a diagram showing MWAA orchestrating Glue jobs reading/writing to S3 buckets)

## Deployment Steps

1.  **Storage**: Create S3 buckets for each layer: `bucket-name-raw`, `bucket-name-bronze`, etc.
2.  **Databases**: Deploy RDS, DocumentDB, and Keyspaces instances within a private VPC.
3.  **Secrets**: Store database credentials in AWS Secrets Manager.
4.  **Orchestration**:
    - Deploy an Amazon MWAA environment.
    - Upload `dags/` to the MWAA S3 bucket.
5.  **Processing**:
    - Package PySpark scripts from `spark/` and upload to S3.
    - Create AWS Glue Jobs or EMR clusters to run these scripts.
6.  **Security**:
    - Use IAM roles with least privilege for MWAA and Glue.
    - Enable S3 server-side encryption (SSE-KMS).
    - Configure VPC Security Groups to restrict traffic.

## CloudFormation / Terraform

Placeholder infrastructure code can be found in `infrastructure/aws/`.
