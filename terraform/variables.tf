
# Variables
# These variables are set per-environment (dev, staging, prod) via .tfvars
# files and are passed down into each module.

variable "aws_region" {
  description = "AWS region where all resources will be deployed"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod). Used in resource naming and tagging."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "project" {
  description = "Project name, used as a prefix in all resource names for namespacing"
  type        = string
  default     = "lakehouse"
}
