# ===========================================================================
# Module: AWS Step Functions
# ===========================================================================
# Creates the state machine that orchestrates the entire ETL pipeline.
# The state machine definition is built using Terraform's templatefile()
# function, which injects the actual Glue job names, S3 paths, and ARNs
# at plan time. This means the state machine is always in sync with the
# infrastructure — no hardcoded ARNs that drift.
#
# Flow:
#   DetectNewFiles → Parallel Glue Jobs (3 branches) → Archive Raw Files
#   → Run Crawler → Athena Validation → Success
#   (Any failure → SNS Alert → Pipeline Failed)

locals {
  prefix              = "${var.project}-${var.environment}"
  archive_lambda_name = "${var.project}-${var.environment}-archive-raw-files"
}

# Archive Lambda function — moves raw files to the archive bucket after ETL
resource "aws_lambda_function" "archive_raw_files" {
  function_name = local.archive_lambda_name
  role          = var.lambda_role_arn
  runtime       = "python3.11"
  handler       = "archive_raw_files.handler"
  timeout       = 120

  s3_bucket = var.scripts_bucket
  s3_key    = "lambda/archive_raw_files.zip"
}

resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "${local.prefix}-etl-pipeline"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${path.module}/state_machine.asl.json", {
    products_job_name    = var.products_glue_job_name
    orders_job_name      = var.orders_glue_job_name
    order_items_job_name = var.order_items_glue_job_name
    crawler_name         = var.crawler_name
    raw_bucket           = var.raw_bucket
    archived_bucket      = var.archived_bucket
    archive_lambda_name  = "${local.prefix}-archive-raw-files"
    athena_workgroup     = var.athena_workgroup
    athena_database      = var.athena_database
    athena_results_loc   = var.athena_results_location
    sns_topic_arn        = var.sns_topic_arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tracing_configuration {
    enabled = true
  }
}

# CloudWatch Log Group for Step Functions execution history
resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/states/${local.prefix}-etl-pipeline"
  retention_in_days = 30
}
