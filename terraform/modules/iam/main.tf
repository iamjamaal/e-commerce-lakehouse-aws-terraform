# ===========================================================================
# Module: IAM Roles & Policies
# ===========================================================================
# Every AWS service in the pipeline needs an IAM role that grants it
# exactly the permissions it needs and nothing more (least privilege).
#
# Roles created:
#   1. Glue Role         — Read raw, write dwh/rejected, read scripts, CloudWatch logs
#   2. Step Functions Role — Start Glue jobs, invoke Lambda, start crawlers, run Athena, publish SNS
#   3. EventBridge Role   — Start the Step Functions state machine
#   4. Lambda Role        — S3 read/write for the archive function
#
# Each role has a trust policy (who can assume it) and a permissions policy
# (what it can do once assumed).

locals {
  prefix = "${var.project}-${var.environment}"
}

# Current AWS account ID and region — used in ARN construction
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ===========================================================================
# 1. Glue ETL Role
# ===========================================================================
# This role is assumed by all three Glue jobs. It needs:
#   - Read access to the raw bucket (to ingest source files)
#   - Read/write access to the DWH bucket (to read/write Delta tables)
#   - Write access to the rejected bucket (to store failed records)
#   - Read access to the scripts bucket (to load ETL code)
#   - CloudWatch Logs (Glue streams job logs here automatically)
#   - Glue Data Catalog (to register/update table metadata)

resource "aws_iam_role" "glue" {
  name = "${local.prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Attach the AWS-managed Glue service role policy (provides baseline Glue permissions)
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy for S3 bucket access (scoped to our specific buckets)
resource "aws_iam_role_policy" "glue_s3" {
  name = "${local.prefix}-glue-s3-policy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadRawBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.raw_bucket_arn,
          "${var.raw_bucket_arn}/*",
        ]
      },
      {
        Sid    = "ReadWriteDwhBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject", # Delta Lake needs delete for VACUUM and log compaction
          "s3:ListBucket",
        ]
        Resource = [
          var.dwh_bucket_arn,
          "${var.dwh_bucket_arn}/*",
        ]
      },
      {
        Sid    = "WriteRejectedBucket"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.rejected_bucket_arn,
          "${var.rejected_bucket_arn}/*",
        ]
      },
      {
        Sid    = "ReadScriptsBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.scripts_bucket_arn,
          "${var.scripts_bucket_arn}/*",
        ]
      },
    ]
  })
}

# CloudWatch Logs — Glue streams job stdout/stderr here
resource "aws_iam_role_policy" "glue_cloudwatch" {
  name = "${local.prefix}-glue-cloudwatch-policy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
      }
    ]
  })
}

# ===========================================================================
# 2. Step Functions Role
# ===========================================================================
# The state machine orchestrates everything, so it needs to invoke each
# downstream service: Glue jobs, Glue crawlers, Lambda (for archiving),
# Athena (for validation), and SNS (for failure alerts).

resource "aws_iam_role" "step_functions" {
  name = "${local.prefix}-step-functions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${local.prefix}-step-functions-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "StartGlueJobs"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
        ]
        Resource = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${var.project}-${var.environment}-*"
      },
      {
        Sid    = "StartGlueCrawler"
        Effect = "Allow"
        Action = [
          "glue:StartCrawler",
          "glue:GetCrawler",
        ]
        Resource = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:crawler/${var.project}-${var.environment}-*"
      },
      {
        Sid    = "InvokeLambda"
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction",
        ]
        Resource = "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.project}-${var.environment}-*"
      },
      {
        Sid    = "RunAthenaQueries"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
        ]
        Resource = "*"
      },
      {
        Sid    = "AthenaS3Results"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.athena_bucket_arn,
          "${var.athena_bucket_arn}/*",
        ]
      },
      {
        Sid    = "AthenaGlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartitions",
        ]
        Resource = "*"
      },
      {
        Sid    = "PublishSNSAlerts"
        Effect = "Allow"
        Action = [
          "sns:Publish",
        ]
        Resource = "arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${var.project}-*"
      },
      {
        Sid    = "CloudWatchEvents"
        Effect = "Allow"
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule",
        ]
        Resource = "arn:aws:events:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:rule/*"
      },
      {
        Sid    = "CloudWatchLogsDelivery"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutLogEvents",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups",
        ]
        Resource = "*"
      },
    ]
  })
}

# ===========================================================================
# 3. EventBridge Role
# ===========================================================================
# EventBridge needs only one permission: start the Step Functions state machine.

resource "aws_iam_role" "eventbridge" {
  name = "${local.prefix}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge" {
  name = "${local.prefix}-eventbridge-policy"
  role = aws_iam_role.eventbridge.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = "arn:aws:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.project}-${var.environment}-*"
      }
    ]
  })
}

# ===========================================================================
# 4. Lambda Role (Archive Function)
# ===========================================================================
# The Lambda that archives raw files after successful ingestion needs
# read access to the raw bucket and write access to the archive bucket.

resource "aws_iam_role" "lambda_archive" {
  name = "${local.prefix}-lambda-archive-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Attach the basic Lambda execution role (CloudWatch Logs)
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_archive.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3" {
  name = "${local.prefix}-lambda-s3-policy"
  role = aws_iam_role.lambda_archive.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadRawAndMoveToArchive"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.raw_bucket_arn,
          "${var.raw_bucket_arn}/*",
        ]
      },
      {
        Sid    = "WriteArchiveBucket"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.archived_bucket_arn,
          "${var.archived_bucket_arn}/*",
        ]
      },
    ]
  })
}
