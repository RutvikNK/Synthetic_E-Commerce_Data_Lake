terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# The Ingestion Layer (Pub/Sub)
resource "google_pubsub_topic" "events_topic" {
  name = "ecommerce-events"
}

# The Storage Layer (Data Lake)
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true # Allows deleting bucket even if it has files (for learning only)

  uniform_bucket_level_access = true
}

# The Analytics Layer (BigQuery)
resource "google_bigquery_dataset" "analytics" {
  dataset_id                  = "ecommerce_analytics"
  friendly_name               = "E-Commerce Analytics"
  description                 = "Synthetic data analysis"
  location                    = var.region
  default_table_expiration_ms = null
}

# The "External Table"
resource "google_bigquery_table" "events" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "raw_events"
  deletion_protection = false

  # Points to GCS bucket
  external_data_configuration {
    autodetect    = false
    source_format = "NEWLINE_DELIMITED_JSON"
    
    # Pattern for Cloud Function to use for file names
    source_uris = [
      "gs://${google_storage_bucket.data_lake.name}/events/*.json"
    ]

    # Point to schema file
    schema = file("../schemas/bq_schema.json")
  }
}