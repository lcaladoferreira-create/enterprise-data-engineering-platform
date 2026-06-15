# Azure Infrastructure - Enterprise Data Platform

# 1. ADLS Gen2 Storage (Hierarchical Namespace)
resource "azurerm_storage_account" "enterprise_datalake" {
  name                     = "enterprisedl${var.environment}"
  resource_group_name      = var.resource_group
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  is_hns_enabled           = true

  network_rules {
    default_action = "Deny"
    bypass         = ["AzureServices"]
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "layers" {
  for_each           = toset(["raw", "bronze", "silver", "gold"])
  name               = each.key
  storage_account_id = azurerm_storage_account.enterprise_datalake.id
}

# 2. Azure Databricks (Compute)
resource "azurerm_databricks_workspace" "spark_workspace" {
  name                = "adb-enterprise-platform-${var.environment}"
  resource_group_name = var.resource_group
  location            = var.location
  sku                 = "premium"
}

# 3. Security (Azure Key Vault)
resource "azurerm_key_vault" "enterprise_secrets" {
  name                = "kv-enterprise-data-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group
  tenant_id           = var.tenant_id
  sku_name            = "premium"

  enabled_for_disk_encryption = true
}

# 4. Monitoring (Log Analytics)
resource "azurerm_log_analytics_workspace" "platform_logs" {
  name                = "law-enterprise-data"
  location            = var.location
  resource_group_name = var.resource_group
  sku                 = "PerGB2018"
  retention_in_days   = 30
}
