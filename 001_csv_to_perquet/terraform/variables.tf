variable "project" {
  description = "data-engineering-gcp-514411"
  type        = string
}

variable "region" {
  description = "Region for GCP resources"
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  type        = string
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  type        = string
}