output "event_rule_arn" {
  description = "ARN of the EventBridge rule that triggers the pipeline"
  value       = aws_cloudwatch_event_rule.s3_new_file.arn
}

output "event_rule_name" {
  value = aws_cloudwatch_event_rule.s3_new_file.name
}
