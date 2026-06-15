# Operations Runbook

This guide is for Data Platform Engineers maintaining the system in production.

## 1. Standard Maintenance

*   **Log Rotation**: Ensure CloudWatch/ELK retention policies are active.
*   **S3 Lifecycle**: Automated transition of Raw data to Glacier after 365 days.
*   **Spark Tuning**: Monitor executor usage and adjust `spark.executor.memory` if OOMs occur.

## 2. Handling Pipeline Failures

### Airflow Task Failure
1.  Check the Airflow UI logs for the specific task instance.
2.  Identify if it's a connectivity issue (Database down) or a code issue (Logic error).
3.  If it's a data issue, check the Raw files for corruption.
4.  Clear the task to retry after fixing the root cause.

### Spark OOM (Out of Memory)
1.  Increase `executor_memory` in the `SparkSubmitOperator`.
2.  If the dataset is large, ensure partitioning is effective and avoid wide transformations without proper shuffling.

## 3. Backfill Procedure

To reprocess data for a specific period:
1.  Set the `catchup=True` or use the Airflow CLI `backfill` command.
2.  Ensure the Raw data for that period is available in the landing zone.
3.  Monitor the Silver layer to ensure no duplicates are introduced (the job is idempotent).

## 4. Disaster Recovery
*   **RPO (Recovery Point Objective)**: 24 hours (daily backup).
*   **RTO (Recovery Time Objective)**: 4 hours (automated infrastructure redeployment via Terraform).
