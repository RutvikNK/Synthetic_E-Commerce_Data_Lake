output "project_id" {
  description = "The GCP Project ID"
  value       = var.project_id
}

output "bucket_name" {
  description = "The name of the GCS bucket created"
  value       = google_storage_bucket.data_lake.name
}

output "topic_name" {
  description = "The name of the Pub/Sub topic"
  value       = google_pubsub_topic.events_topic.name
}