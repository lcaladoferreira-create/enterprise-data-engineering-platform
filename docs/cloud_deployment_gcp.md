# Cloud Deployment: GCP

## Architecture Mapping
*   **Storage**: Google Cloud Storage (GCS) - regional buckets for medallion layers.
*   **Compute**: Cloud Dataproc (Managed Spark) or Cloud Dataflow (Beam).
*   **Orchestration**: Cloud Composer (Managed Airflow).
*   **Database**: Cloud SQL (Postgres/MySQL), Firestore, Cloud Bigtable.
*   **Security**: IAM, Secret Manager, Cloud KMS.
*   **Analytics**: BigQuery (Serving Gold layer).

## Deployment Steps
1.  **Project Setup**: Create a GCP project and enable APIs for Composer, Dataproc, and GCS.
2.  **IaC**: Run the Terraform module in `infrastructure/gcp/` to provision GCS and BigQuery.
3.  **Secrets**: Add database credentials to Secret Manager.
4.  **Airflow**: Deploy Cloud Composer and sync the `dags/` folder.
5.  **Processing**: Submit PySpark jobs to Dataproc using the `DataprocSubmitPySparkJobOperator`.
