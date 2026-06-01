variable "environment" {
  type = string
}

variable "project" {
  type = string
}

variable "raw_bucket_arn" {
  description = "ARN of the raw landing zone S3 bucket"
  type        = string
}

variable "dwh_bucket_arn" {
  description = "ARN of the DWH (Delta Lake) S3 bucket"
  type        = string
}

variable "archived_bucket_arn" {
  description = "ARN of the archived raw files S3 bucket"
  type        = string
}

variable "rejected_bucket_arn" {
  description = "ARN of the rejected records S3 bucket"
  type        = string
}

variable "scripts_bucket_arn" {
  description = "ARN of the Glue scripts S3 bucket"
  type        = string
}

variable "athena_bucket_arn" {
  description = "ARN of the Athena query results S3 bucket"
  type        = string
}
