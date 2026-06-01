variable "environment" {
  type = string
}

variable "project" {
  type = string
}

variable "step_functions_role_arn" {
  description = "ARN of the IAM role for Step Functions"
  type        = string
}

variable "products_glue_job_name" {
  description = "Name of the Glue ETL job for products"
  type        = string
}

variable "orders_glue_job_name" {
  description = "Name of the Glue ETL job for orders"
  type        = string
}

variable "order_items_glue_job_name" {
  description = "Name of the Glue ETL job for order items"
  type        = string
}

variable "crawler_name" {
  description = "Name of the Glue Crawler"
  type        = string
}

variable "raw_bucket" {
  description = "Name of the raw S3 bucket"
  type        = string
}

variable "archived_bucket" {
  description = "Name of the archive S3 bucket"
  type        = string
}

variable "athena_workgroup" {
  description = "Athena workgroup name"
  type        = string
}

variable "athena_database" {
  description = "Glue Data Catalog database name for Athena queries"
  type        = string
}

variable "athena_results_location" {
  description = "S3 path for Athena query results"
  type        = string
}

variable "sns_topic_arn" {
  description = "ARN of the SNS topic for pipeline failure alerts"
  type        = string
}

variable "lambda_role_arn" {
  description = "ARN of the IAM role for the archive Lambda function"
  type        = string
}

variable "scripts_bucket" {
  description = "Name of the S3 bucket holding Glue scripts and Lambda zips"
  type        = string
}
