# Cloud Deployment: Azure

## Architecture Mapping
*   **Storage**: Azure Data Lake Storage (ADLS) Gen2.
*   **Compute**: Azure Databricks or Azure Synapse Analytics (Spark pools).
*   **Orchestration**: Azure Data Factory (Managed Airflow integration).
*   **Database**: Azure Database for PostgreSQL/MySQL, Azure Cosmos DB.
*   **Security**: Microsoft Entra ID (Azure AD), Azure Key Vault.
*   **Monitoring**: Azure Monitor, Log Analytics.

## Deployment Steps
1.  **Storage**: Run Terraform in `infrastructure/azure/` to create ADLS Gen2 containers for each medallion layer.
2.  **Secrets**: Store all connection strings in Azure Key Vault.
3.  **Processing**: Set up an Azure Databricks workspace. Use the Databricks clusters to execute the PySpark code from the `spark/` directory.
4.  **Orchestration**: Configure Azure Data Factory to trigger the Airflow DAGs.

## Enterprise Features
*   Use **Private Links** to ensure storage and database traffic does not traverse the public internet.
*   Integrate with **Microsoft Purview** for automated data discovery and lineage.
