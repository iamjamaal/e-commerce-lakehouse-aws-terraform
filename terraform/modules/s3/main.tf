# ===========================================================================
# Module: S3 Buckets
# ===========================================================================
# Creates the six S3 buckets that form the storage backbone of the lakehouse:
#
#   1. lakehouse-raw      — Landing zone where new CSV/XLSX files are dropped
#   2. lakehouse-dwh      — Processed Delta Lake tables (the "warehouse")
#   3. lakehouse-archived — Raw files moved here after successful ETL
#   4. lakehouse-rejected — Records that failed validation (for investigation)
#   5. lakehouse-scripts  — Glue ETL scripts uploaded by CI/CD
#   6. lakehouse-athena   — Athena query result output location
#
# Each bucket has server-side encryption (SSE-S3), versioning on the DWH
# bucket (Delta Lake needs it for time travel), and lifecycle rules to
# manage storage costs (e.g., archive old rejected records to Glacier).

# ── Local variables for consistent naming ─────────────────────────────
data "aws_caller_identity" "current" {}

locals {
  # Appending account ID ensures bucket names are globally unique across all AWS accounts.
  prefix = "${var.project}-${var.environment}-${data.aws_caller_identity.current.account_id}"
}

# ── 1. Raw Landing Zone ───────────────────────────────────────────────
resource "aws_s3_bucket" "raw" {
  bucket        = "${local.prefix}-raw"
  force_destroy = var.environment != "prod" # Safety: prevent accidental deletion in prod
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# EventBridge notification — required for the S3 → EventBridge → Step Functions trigger
resource "aws_s3_bucket_notification" "raw_eventbridge" {
  bucket      = aws_s3_bucket.raw.id
  eventbridge = true
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── 2. DWH (Delta Lake Tables) ────────────────────────────────────────
resource "aws_s3_bucket" "dwh" {
  bucket        = "${local.prefix}-dwh"
  force_destroy = var.environment != "prod"
}

# Versioning is critical for the DWH bucket — Delta Lake uses S3 versioning
# for its transaction log, enabling time travel and rollback.
resource "aws_s3_bucket_versioning" "dwh" {
  bucket = aws_s3_bucket.dwh.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dwh" {
  bucket = aws_s3_bucket.dwh.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "dwh" {
  bucket                  = aws_s3_bucket.dwh.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── 3. Archived Raw Files ─────────────────────────────────────────────
resource "aws_s3_bucket" "archived" {
  bucket        = "${local.prefix}-archived"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archived" {
  bucket = aws_s3_bucket.archived.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle rule: move archived files to Glacier after 90 days (cost savings)
resource "aws_s3_bucket_lifecycle_configuration" "archived" {
  bucket = aws_s3_bucket.archived.id

  rule {
    id     = "glacier-after-90-days"
    status = "Enabled"
    filter {}

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365 # Delete after 1 year
    }
  }
}

resource "aws_s3_bucket_public_access_block" "archived" {
  bucket                  = aws_s3_bucket.archived.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── 4. Rejected Records ───────────────────────────────────────────────
resource "aws_s3_bucket" "rejected" {
  bucket        = "${local.prefix}-rejected"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "rejected" {
  bucket = aws_s3_bucket.rejected.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle: expire rejected records after 30 days (they're for debugging, not long-term)
resource "aws_s3_bucket_lifecycle_configuration" "rejected" {
  bucket = aws_s3_bucket.rejected.id

  rule {
    id     = "expire-after-30-days"
    status = "Enabled"
    filter {}

    expiration {
      days = 30
    }
  }
}

resource "aws_s3_bucket_public_access_block" "rejected" {
  bucket                  = aws_s3_bucket.rejected.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── 5. Glue ETL Scripts ───────────────────────────────────────────────
resource "aws_s3_bucket" "scripts" {
  bucket        = "${local.prefix}-glue-scripts"
  force_destroy = true # Scripts are always re-uploadable from git
}

resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket                  = aws_s3_bucket.scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── 6. Athena Query Results ───────────────────────────────────────────
resource "aws_s3_bucket" "athena_results" {
  bucket        = "${local.prefix}-athena-results"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle: expire Athena results after 7 days (they're ephemeral query output)
resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-after-7-days"
    status = "Enabled"
    filter {}

    expiration {
      days = 7
    }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
