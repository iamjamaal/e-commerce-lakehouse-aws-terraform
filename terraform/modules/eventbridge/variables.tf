variable "environment" {
  type = string
}

variable "project" {
  type = string
}

variable "raw_bucket_name" {
  description = "Name of the raw S3 bucket to watch for new file uploads"
  type        = string
}

variable "state_machine_arn" {
  description = "ARN of the Step Functions state machine to trigger"
  type        = string
}

variable "eventbridge_role_arn" {
  description = "ARN of the IAM role that EventBridge uses to start Step Functions"
  type        = string
}
