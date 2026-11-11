terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

variable "project" {
  description = "GCP Project ID"
  type        = string
}

provider "google" {
  project = var.project
  region  = "us-central1"
}

resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${var.project}-terra-bucket"
  location      = "US"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = "trips_data_all"
  project    = var.project
  location   = "US"
}