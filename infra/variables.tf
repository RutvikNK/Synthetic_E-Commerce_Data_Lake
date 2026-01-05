variable "project_id" {
  description = "Your Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region for resources"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "The unique name for your Data Lake bucket"
  type        = string
  default     = "ecommerce-datalake-demo-12345" 
}