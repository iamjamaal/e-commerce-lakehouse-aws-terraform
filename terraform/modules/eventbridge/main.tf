# ===========================================================================
# Module: EventBridge (S3 → Step Functions Trigger)
# ===========================================================================
# This module sets up the automatic trigger that starts the ETL pipeline
# whenever a new file lands in the raw S3 bucket.
#
# How it works:
#   1. The S3 raw bucket has EventBridge notifications enabled (set in the S3 module).
#   2. When an object is created (PutObject, CompleteMultipartUpload), S3
#      sends an event to EventBridge.
#   3. The EventBridge rule below matches that event pattern and routes it
#      to the Step Functions state machine as the target.
#   4. The state machine starts executing with the S3 event payload, which
#      contains the bucket name, key, and object size.
#
# The event pattern filters for specific prefixes (products/, orders/,
# order_items/) so that random files dropped in the bucket root don't
# trigger the pipeline unnecessarily.

locals {
  prefix = "${var.project}-${var.environment}"
}

# EventBridge rule — matches S3 object-created events in the raw bucket
resource "aws_cloudwatch_event_rule" "s3_new_file" {
  name        = "${local.prefix}-raw-file-trigger"
  description = "Triggers the lakehouse ETL pipeline when new files land in the raw S3 bucket"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.raw_bucket_name]
      }
      object = {
        # Only trigger for files in our expected dataset prefixes
        key = [
          { prefix = "products/" },
          { prefix = "orders/" },
          { prefix = "order_items/" },
        ]
      }
    }
  })
}

# Target — route the matched event to the Step Functions state machine
resource "aws_cloudwatch_event_target" "start_pipeline" {
  rule      = aws_cloudwatch_event_rule.s3_new_file.name
  target_id = "${local.prefix}-start-etl"
  arn       = var.state_machine_arn
  role_arn  = var.eventbridge_role_arn

  # Pass the full S3 event detail to the state machine as input.
  # The state machine's DetectNewFiles state can read $.detail.bucket.name
  # and $.detail.object.key to know which file triggered the run.
  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }
    input_template = <<-EOT
      {
        "trigger": "s3_event",
        "source_bucket": <bucket>,
        "source_key": <key>,
        "datasets": ["products", "orders", "order_items"]
      }
    EOT
  }
}
