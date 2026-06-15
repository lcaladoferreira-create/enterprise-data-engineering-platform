# Azure Infrastructure Variables

variable "resource_group" {
  description = "Azure Resource Group"
  type        = string
}

variable "location" {
  description = "Azure Region"
  type        = string
  default     = "East US"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "tenant_id" {
  description = "Azure Tenant ID"
  type        = string
}
