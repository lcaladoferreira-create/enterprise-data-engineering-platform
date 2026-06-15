# Observability and Monitoring

## Logging Strategy
- **Structured Logging**: All Python and Spark jobs use structured JSON logging to allow easy parsing by ELK or Splunk.
- **Airflow Logs**: Captured for every task execution, including retries and failures.
- **NiFi Provenance**: Detailed tracking of data lineage.

## Metrics
- **Pipeline Latency**: Time taken for data to move from Raw to Gold.
- **Data Volume**: Number of records processed per layer.
- **Success/Failure Rate**: Percentage of successful vs failed Airflow tasks.
- **Data Quality Score**: Number of records passing vs failing DQ checks.

## Monitoring & Alerting
- **CloudWatch / Azure Monitor**: Infrastructure metrics (CPU, Memory, Disk).
- **SLA Alerts**: Triggered if the Gold layer is not refreshed within the expected time.
- **Failure Alerts**: Slack/Email notifications on Airflow task failure.

## Troubleshooting Guide
1.  **Spark Job Fails**: Check `airflow_logs` and Spark UI for executor OOM or shuffle issues.
2.  **NiFi Connection Error**: Verify database service is running and firewall/security groups allow traffic.
3.  **Data Quality Failure**: Inspect `sql/quality_checks/` results to identify which business rule was violated.
