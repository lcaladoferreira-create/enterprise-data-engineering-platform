# Azure Deployment Guide

This document outlines the architecture and deployment steps for running the platform on Microsoft Azure.

## Architecture Mapping

| Local Component | Azure Equivalent |
|-----------------|----------------|
| Raw/Bronze/Silver/Gold | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| Apache Spark | Azure Databricks / Azure Synapse Spark |
| Apache Airflow | Azure Data Factory (Managed Airflow) |
| PostgreSQL/MySQL | Azure Database for PostgreSQL/MySQL |
| MongoDB | Azure Cosmos DB (API for MongoDB) |
| Cassandra | Azure Cosmos DB (API for Cassandra) |
| Secrets | Azure Key Vault |
| Security | Microsoft Entra ID (Azure AD), RBAC |
| Logging | Azure Monitor / Log Analytics |

## Deployment Steps

1.  **Storage**: Create an ADLS Gen2 account and containers for `raw`, `bronze`, `silver`, and `gold`.
2.  **Databases**: Provision Azure Database for PostgreSQL/MySQL and Cosmos DB instances.
3.  **Orchestration**:
    - Deploy Azure Data Factory with Managed Airflow.
    - Connect the Airflow environment to the Git repository.
4.  **Processing**:
    - Set up an Azure Databricks workspace.
    - Use the `DatabricksSubmitRunOperator` to trigger Spark jobs.
5.  **Security**:
    - Use Managed Identities for secure service-to-service communication.
    - Store all connection strings in Azure Key Vault.

## Bicep / Terraform

Placeholder infrastructure code can be found in `infrastructure/azure/`.
