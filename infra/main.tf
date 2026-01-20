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

locals {
  event_types = toset(["ad_click", "page_view", "add_to_cart", "purchase"])
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# The Ingestion Layer
resource "google_pubsub_topic" "events_topic" {
  name = "ecommerce-events"
}

# Data lake
resource "google_storage_bucket" "main" {
  name                        = "${var.bucket_name}-${random_id.bucket_suffix.hex}"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# The Analytics Layer
resource "google_bigquery_dataset" "analytics" {
  dataset_id                  = "ecommerce_analytics"
  friendly_name               = "E-Commerce Analytics"
  location                    = var.region
}

# Placeholder partition
resource "google_storage_bucket_object" "placeholder" {
  for_each = local.event_types
  
  # The "Folder" structure is defined here in the name
  name    = "event_type=${each.key}/year=2000/month=01/day=01/init.json"
  
  # Point to the single main bucket
  bucket  = google_storage_bucket.main.name
  
  content = jsonencode({
    event_id     = "00000000-0000-0000-0000-000000000000"
    event_type   = each.key
    user_id      = "system_init"
    timestamp    = "2000-01-01T00:00:00Z"
    device       = "system"
    location     = "US"
    product_id   = null
    product_name = null
    category     = null
    price        = 0.0
    ad_source    = null
    campaign_id  = null
  })
}

# External tables
resource "google_bigquery_table" "event_tables" {
  for_each   = local.event_types
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = "raw_${each.key}"
  
  deletion_protection = false
  depends_on          = [google_storage_bucket_object.placeholder]
  schema              = file("../schemas/bq_schema.json")

  external_data_configuration {
    autodetect    = false 
    source_format = "NEWLINE_DELIMITED_JSON"
    
    # Point ONLY to the specific event_type folder inside the main bucket
    source_uris = ["gs://${google_storage_bucket.main.name}/event_type=${each.key}/*"]
    
    hive_partitioning_options {
      mode              = "AUTO"
      # BigQuery treats this folder as the "Root" for this specific table
      source_uri_prefix = "gs://${google_storage_bucket.main.name}/event_type=${each.key}"
    }
  }
}

# Quarantine Layer
resource "google_storage_bucket" "quarantine" {
  name          = "${var.bucket_name}-quarantine-${random_id.bucket_suffix.hex}"
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