output "glue_role_arn" {
  description = "ARN of the IAM role for Glue ETL jobs"
  value       = aws_iam_role.glue.arn
}

output "glue_role_name" {
  value = aws_iam_role.glue.name
}

output "step_functions_role_arn" {
  description = "ARN of the IAM role for Step Functions"
  value       = aws_iam_role.step_functions.arn
}

output "eventbridge_role_arn" {
  description = "ARN of the IAM role for EventBridge"
  value       = aws_iam_role.eventbridge.arn
}

output "lambda_archive_role_arn" {
  description = "ARN of the IAM role for the archive Lambda"
  value       = aws_iam_role.lambda_archive.arn
}
