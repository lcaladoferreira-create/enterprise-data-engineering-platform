# Azure Infrastructure Placeholders

resource "azurerm_storage_account" "datalake" {
  name                     = "datalakestorage"
  resource_group_name      = "data-platform-rg"
  location                 = "East US"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

resource "azurerm_databricks_workspace" "spark" {
  name                = "databricks-workspace"
  resource_group_name = "data-platform-rg"
  location            = "East US"
  sku                 = "standard"
}
