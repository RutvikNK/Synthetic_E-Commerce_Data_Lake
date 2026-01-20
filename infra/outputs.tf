output "project_id" {
  description = "The GCP Project ID"
  value       = var.project_id
}

output "topic_name" {
  description = "The name of the Pub/Sub topic"
  value       = google_pubsub_topic.events_topic.name
}

output "bucket_map" {
  description = "Names of GCS buckets"
  value = { for k, v in google_storage_bucket.event_buckets : k => v.name }
}

output "quarantine_bucket" {
  description = "Name of quarantine bucket"
  value = google_storage_bucket.quarantine.name
}