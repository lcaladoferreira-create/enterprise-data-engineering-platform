# GCP Infrastructure for Data Engineering Platform

provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. GCS Buckets (Medallion Architecture)
resource "google_storage_bucket" "datalake" {
  for_each      = toset(["raw", "bronze", "silver", "gold"])
  name          = "enterprise-datalake-${each.key}-${var.environment}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

# 2. BigQuery Datasets for Gold Layer
resource "google_bigquery_dataset" "gold_layer" {
  dataset_id                  = "gold_star_schema"
  friendly_name               = "Gold Layer Star Schema"
  description                 = "Curated analytical tables for reporting"
  location                    = var.region
}

# 3. Cloud Dataproc Cluster for Spark
resource "google_dataproc_cluster" "spark_cluster" {
  name   = "data-engineering-cluster-${var.environment}"
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
    }
    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-2"
    }
  }
}

# 4. Secret Manager for Credentials
resource "google_secret_manager_secret" "db_password" {
  secret_id = "source-db-password"
  replication {
    automatic = true
  }
}
