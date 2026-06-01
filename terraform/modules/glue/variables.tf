variable "environment" {
  type = string
}

variable "project" {
  type = string
}

variable "glue_role_arn" {
  description = "ARN of the IAM role for Glue jobs"
  type        = string
}

variable "scripts_bucket" {
  description = "Name of the S3 bucket containing Glue ETL scripts"
  type        = string
}

variable "raw_bucket" {
  description = "Name of the raw landing zone S3 bucket"
  type        = string
}

variable "dwh_bucket" {
  description = "Name of the DWH S3 bucket"
  type        = string
}

variable "rejected_bucket" {
  description = "Name of the rejected records S3 bucket"
  type        = string
}

variable "dwh_bucket_path" {
  description = "Full S3 path to the DWH bucket (for crawler targets)"
  type        = string
}

variable "glue_worker_count" {
  description = "Number of Glue workers per job. 2 for dev, 5-10 for prod."
  type        = number
  default     = 2
}
