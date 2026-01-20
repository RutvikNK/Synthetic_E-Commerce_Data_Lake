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

# Event types
locals {
  event_types = toset(["ad_click", "page_view", "add_to_cart", "purchase"])
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# The Ingestion Layer (Pub/Sub)
resource "google_pubsub_topic" "events_topic" {
  name = "ecommerce-events"
}

# The Storage Layer (Data Lake)
resource "google_storage_bucket" "event_buckets" {
  for_each                    = local.event_types
  name                        = "ecommerce-${each.key}-${random_id.bucket_suffix.hex}"
  location                    = var.region
  force_destroy               = true
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

# Placeholder resource that hold partion structure
resource "google_storage_bucket_object" "placeholder" {
  for_each = local.event_types
  
  # Create a fake historical partition so BigQuery recognizes the structure
  name    = "year=2000/month=01/day=01/init.json"
  content = "{\"status\": \"initialized\"}"
  bucket  = google_storage_bucket.event_buckets[each.key].name
}

# The "External Table"
resource "google_bigquery_table" "event_tables" {
  for_each   = local.event_types
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = "raw_${each.key}"  # e.g., raw_purchase
  
  depends_on = [google_storage_bucket_object.placeholder]
  schema = file("../schemas/bq_schema.json")

  external_data_configuration {
    autodetect    = false 
    source_format = "NEWLINE_DELIMITED_JSON"
    source_uris   = ["gs://${google_storage_bucket.event_buckets[each.key].name}/*"]
    
    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${google_storage_bucket.event_buckets[each.key].name}"
    }
  }
}

# Quarantine Layer (Dead Letter Queue Bucket)
resource "google_storage_bucket" "quarantine" {
  name          = "ecommerce-quarantine-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = true

  # Auto-delete bad files after 14 days
  lifecycle_rule {
    condition {
      age = 14
    }
    action {
      type = "Delete"
    }
  }
}