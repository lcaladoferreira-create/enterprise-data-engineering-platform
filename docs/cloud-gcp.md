# GCP Deployment Guide

This document outlines the architecture and deployment steps for running the platform on Google Cloud Platform.

## Architecture Mapping

| Local Component | GCP Equivalent |
|-----------------|----------------|
| Raw/Bronze/Silver/Gold | Google Cloud Storage (GCS) |
| Apache Spark | Cloud Dataproc / Cloud Run (Spark on K8s) |
| Apache Airflow | Cloud Composer |
| PostgreSQL/MySQL | Cloud SQL |
| MongoDB | Firestore (Datastore mode) or MongoDB Atlas |
| Cassandra | Cloud Bigtable (compatible) |
| Secrets | Secret Manager |
| Security | Cloud IAM, Cloud KMS |
| Logging | Cloud Logging |

## Deployment Steps

1.  **Storage**: Create GCS buckets for the data layers.
2.  **Databases**: Deploy Cloud SQL instances.
3.  **Orchestration**:
    - Create a Cloud Composer environment.
    - Synchronize the `dags/` folder to the Composer GCS bucket.
4.  **Processing**:
    - Use Dataproc for batch processing.
    - Submit Spark jobs using the `DataprocSubmitPySparkJobOperator` in Airflow.
5.  **Data Ingestion**:
    - Use Dataflow or NiFi on GCE.
6.  **Security**:
    - Use Service Accounts with minimal IAM roles.
    - Use Cloud KMS for managed encryption keys.

## Terraform

Placeholder infrastructure code can be found in `infrastructure/gcp/`.
