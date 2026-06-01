variable "environment" {
  type = string
}

variable "project" {
  type = string
}

variable "athena_results_bucket" {
  description = "Name of the S3 bucket for Athena query results"
  type        = string
}
