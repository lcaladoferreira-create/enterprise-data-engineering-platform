# Production Hardening Checklist

Before moving this platform to a production environment, ensure the following items are completed:

## Infrastructure & Connectivity
- [ ] Implement Terraform state locking with DynamoDB/GCS/Azure Blob.
- [ ] Enable VPC peering or Transit Gateway between Ingestion and Storage VPCs.
- [ ] Configure private DNS zones for all internal services.

## Security
- [ ] Rotate all database passwords every 90 days via Secrets Manager rotation Lambda.
- [ ] Implement Row-Level Security (RLS) in the final Gold serving layer.
- [ ] Perform a PII audit to ensure no unmasked sensitive data reached the Silver/Gold layers.
- [ ] Enable GuardDuty (AWS) or Security Command Center (GCP) for threat detection.

## Performance
- [ ] Optimize Spark Shuffle Partitions based on data volume.
- [ ] Enable Spark Dynamic Allocation.
- [ ] Use Z-Order or Liquid Clustering (if using Delta Lake) for performance on filtered columns.

## Monitoring & Alerting
- [ ] Set up Slack/PagerDuty integration for Airflow failure callbacks.
- [ ] Create a Grafana dashboard for Data Quality trends.
- [ ] Configure cost alerts for BigQuery/S3 usage.

## Compliance
- [ ] Document the data lineage from source to mart.
- [ ] Implement an automated "Right to be Forgotten" process for customer deletion requests.
