# GCP Infrastructure Placeholders

resource "google_storage_bucket" "data_lake" {
  name     = "multi-cloud-data-platform-datalake"
  location = "US"
}

resource "google_composer_environment" "airflow" {
  name   = "data-pipeline-composer"
  region = "us-central1"
}
