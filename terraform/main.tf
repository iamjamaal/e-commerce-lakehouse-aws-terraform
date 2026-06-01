
# Lakehouse Infrastructure — Root Configuration
# ===========================================================================
# This is the entry point for the Terraform deployment. It wires together
# all the infrastructure modules (S3, IAM, Glue, Step Functions, Athena,
# EventBridge) and passes outputs between them so that, for example, the
# Glue module knows which IAM role to attach and which S3 buckets to read
# from/write to.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state in S3 with DynamoDB locking — prevents concurrent applies
  # from corrupting state. The bucket and table must be created beforehand
  # (a bootstrap step, typically done once manually or via a separate TF config).
  backend "s3" {
    bucket         = "lakehouse-terraform-state-834424012278"
    key            = "lakehouse/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "lakehouse-terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "lakehouse"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}


# S3 Buckets
# The four storage zones: raw (landing), dwh (Delta tables),
# archived (post-ingestion), and rejected (failed validation records).
# Also creates the Glue scripts bucket and the Athena query results bucket.
module "s3" {
  source      = "./modules/s3"
  environment = var.environment
  project     = var.project
}



# IAM Roles & Policies
# IAM roles for Glue jobs, Step Functions, EventBridge, and
# Lambda. Each role follows least-privilege: Glue can read raw + write dwh,
# Step Functions can invoke Glue + Lambda, etc.
module "iam" {
  source      = "./modules/iam"
  environment = var.environment
  project     = var.project

  # Pass bucket ARNs so policies can scope permissions to specific buckets
  raw_bucket_arn      = module.s3.raw_bucket_arn
  dwh_bucket_arn      = module.s3.dwh_bucket_arn
  archived_bucket_arn = module.s3.archived_bucket_arn
  rejected_bucket_arn = module.s3.rejected_bucket_arn
  scripts_bucket_arn  = module.s3.scripts_bucket_arn
  athena_bucket_arn   = module.s3.athena_results_bucket_arn
}



#AWS Glue (ETL Jobs + Crawler + Data Catalog)
# The three Glue jobs (products, orders, order_items), a Glue database
# in the Data Catalog, and a Crawler that registers Delta tables as catalog
# tables for Athena to query.
module "glue" {
  source      = "./modules/glue"
  environment = var.environment
  project     = var.project

  glue_role_arn      = module.iam.glue_role_arn
  scripts_bucket     = module.s3.scripts_bucket_name
  raw_bucket         = module.s3.raw_bucket_name
  dwh_bucket         = module.s3.dwh_bucket_name
  rejected_bucket    = module.s3.rejected_bucket_name
  dwh_bucket_path    = "s3://${module.s3.dwh_bucket_name}/"
}



# AWS Step Functions (Orchestration)
# Creates the state machine that orchestrates the ETL pipeline:
# detect files → parallel Glue jobs → archive → crawl → validate via Athena.
module "step_functions" {
  source      = "./modules/step_functions"
  environment = var.environment
  project     = var.project

  step_functions_role_arn = module.iam.step_functions_role_arn

  # Glue job names so the state machine can reference them
  products_glue_job_name    = module.glue.products_job_name
  orders_glue_job_name      = module.glue.orders_job_name
  order_items_glue_job_name = module.glue.order_items_job_name
  crawler_name              = module.glue.crawler_name

  # S3 paths for the archive Lambda
  raw_bucket      = module.s3.raw_bucket_name
  archived_bucket = module.s3.archived_bucket_name

  # Athena validation
  athena_workgroup        = module.athena.workgroup_name
  athena_database         = module.glue.database_name
  athena_results_location = "s3://${module.s3.athena_results_bucket_name}/query-results/"

  # SNS for failure alerting
  sns_topic_arn = aws_sns_topic.pipeline_alerts.arn

  # Lambda archive function
  lambda_role_arn = module.iam.lambda_archive_role_arn
  scripts_bucket  = module.s3.scripts_bucket_name
}




# Module: EventBridge (S3 Event Trigger)
# Sets up an EventBridge rule that watches the raw S3 bucket for new object
# uploads and triggers the Step Functions state machine automatically.
module "eventbridge" {
  source      = "./modules/eventbridge"
  environment = var.environment
  project     = var.project

  raw_bucket_name        = module.s3.raw_bucket_name
  state_machine_arn      = module.step_functions.state_machine_arn
  eventbridge_role_arn   = module.iam.eventbridge_role_arn
}


# Module: Amazon Athena
# Creates an Athena workgroup with query result location, byte-scan limits,
# and engine version pinned for consistency.
module "athena" {
  source      = "./modules/athena"
  environment = var.environment
  project     = var.project

  athena_results_bucket = module.s3.athena_results_bucket_name
}


# SNS Topic for Pipeline Alerts
# Step Functions publishes to this topic on pipeline failure. Subscribers
# (email, Slack webhook, PagerDuty) are added manually or via additional TF.
resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project}-${var.environment}-pipeline-alerts"
}
