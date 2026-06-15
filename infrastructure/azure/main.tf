# Azure Infrastructure for Data Engineering Platform

# 1. ADLS Gen2 Storage Account
resource "azurerm_storage_account" "datalake" {
  name                     = "enterprisedatalake${var.environment}"
  resource_group_name      = var.resource_group
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Critical for ADLS Gen2
}

# 2. File Systems (Containers) for Medallion Layers
resource "azurerm_storage_data_lake_gen2_filesystem" "layers" {
  for_each           = toset(["raw", "bronze", "silver", "gold"])
  name               = each.key
  storage_account_id = azurerm_storage_account.datalake.id
}

# 3. Azure Databricks Workspace
resource "azurerm_databricks_workspace" "spark_env" {
  name                = "adb-enterprise-data-${var.environment}"
  resource_group_name = var.resource_group
  location            = var.location
  sku                 = "premium"
}

# 4. Azure Key Vault
resource "azurerm_key_vault" "secrets" {
  name                = "kv-data-platform-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group
  tenant_id           = var.tenant_id
  sku_name            = "standard"
}

# 5. Log Analytics Workspace
resource "azurerm_log_analytics_workspace" "monitoring" {
  name                = "law-data-platform"
  location            = var.location
  resource_group_name = var.resource_group
  sku                 = "PerGB2018"
}
