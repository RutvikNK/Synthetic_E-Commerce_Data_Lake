output "project_id" {
  description = "The GCP Project ID"
  value       = var.project_id
}

output "topic_name" {
  description = "The name of the Pub/Sub topic"
  value       = google_pubsub_topic.events_topic.name
}

output "bucket_name" {
  description = "The name of the GCS bucket"
  value = google_storage_bucket.main.name
}

output "quarantine_bucket" {
  description = "The name of the GCS quarantine bucket"
  value = google_storage_bucket.quarantine.name
}