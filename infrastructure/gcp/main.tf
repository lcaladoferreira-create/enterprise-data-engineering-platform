# GCP Infrastructure - Enterprise Data Platform

# 1. Cloud Storage for Data Lake
resource "google_storage_bucket" "datalake_buckets" {
  for_each      = toset(["raw", "bronze", "silver", "gold"])
  name          = "enterprise-datalake-${each.key}-${var.environment}"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  encryption {
    default_kms_key_name = google_kms_crypto_key.storage_key.id
  }
}

# 2. BigQuery Serving Layer (Gold)
resource "google_bigquery_dataset" "serving_layer" {
  dataset_id                  = "enterprise_gold_layer"
  friendly_name               = "Enterprise Serving Layer"
  description                 = "Curated analytical marts for downstream consumption"
  location                    = var.region
  delete_contents_on_destroy = false
}

# 3. Cloud Dataproc (Managed Spark Cluster)
resource "google_dataproc_cluster" "spark_cluster" {
  name   = "enterprise-data-cluster-${var.environment}"
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"
    }

    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-4"
      disk_config {
        boot_disk_size_gb = 100
      }
    }

    software_config {
      image_version = "2.1-debian11"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "false"
      }
    }
  }
}

# 4. Security (KMS)
resource "google_kms_key_ring" "keyring" {
  name     = "enterprise-data-keyring"
  location = var.region
}

resource "google_kms_crypto_key" "storage_key" {
  name     = "storage-encryption-key"
  key_ring = google_kms_key_ring.keyring.id
}
