
# Outputs
# For CI/CD pipelines (GitHub Actions needs the
# state machine ARN and scripts bucket name) and for operators who need
# to reference resources by name or ARN.

output "raw_bucket_name" {
  description = "S3 bucket for the raw landing zone"
  value       = module.s3.raw_bucket_name
}

output "dwh_bucket_name" {
  description = "S3 bucket for Delta Lake tables (lakehouse DWH)"
  value       = module.s3.dwh_bucket_name
}

output "archived_bucket_name" {
  description = "S3 bucket for archived raw files"
  value       = module.s3.archived_bucket_name
}

output "scripts_bucket_name" {
  description = "S3 bucket where Glue ETL scripts are stored"
  value       = module.s3.scripts_bucket_name
}

output "state_machine_arn" {
  description = "ARN of the Step Functions state machine (needed by CI/CD)"
  value       = module.step_functions.state_machine_arn
}

output "glue_database_name" {
  description = "Glue Data Catalog database name"
  value       = module.glue.database_name
}

output "athena_workgroup" {
  description = "Athena workgroup for running analytics queries"
  value       = module.athena.workgroup_name
}

output "sns_topic_arn" {
  description = "SNS topic ARN for pipeline failure alerts"
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "products_glue_job_name" {
  value = module.glue.products_job_name
}

output "orders_glue_job_name" {
  value = module.glue.orders_job_name
}

output "order_items_glue_job_name" {
  value = module.glue.order_items_job_name
}
