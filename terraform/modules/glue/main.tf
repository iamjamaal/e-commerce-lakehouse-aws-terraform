# ===========================================================================
# Module: AWS Glue (ETL Jobs + Data Catalog + Crawler)
# ===========================================================================
# This module creates:
#   1. A Glue Data Catalog database (the metadata layer that Athena queries)
#   2. Three Glue ETL jobs (one per dataset: products, orders, order_items)
#   3. A Glue Crawler that walks the DWH bucket and registers Delta tables
#      in the Data Catalog, so Athena can discover and query them.
#
# Each Glue job is configured as a Spark job with Delta Lake JARs, running
# on G.1X workers (4 vCPU, 16 GB each). The worker count is kept low for
# dev/staging and can be scaled up for production via the variable.

locals {
  prefix = "${var.project}-${var.environment}"
}

# ===========================================================================
# Glue Data Catalog Database
# ===========================================================================
# This is the logical namespace in Athena. All three tables (dim_products,
# fact_orders, fact_order_items) are registered under this database.
resource "aws_glue_catalog_database" "lakehouse" {
  name = "${var.project}_${var.environment}_dwh"

  description = "Lakehouse DWH — Delta Lake tables for e-commerce analytics"
}

# ===========================================================================
# Glue ETL Job: Products
# ===========================================================================
resource "aws_glue_job" "products" {
  name     = "${local.prefix}-etl-products"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/etl/jobs/etl_products.py"
    python_version  = "3"
  }

  # Use Glue 4.0 native Delta Lake support (avoids JAR/Python version mismatch)
  default_arguments = {
    "--datalake-formats"              = "delta"
    "--extra-py-files"                = "s3://${var.scripts_bucket}/etl.zip"
    "--raw_path"                      = "s3://${var.raw_bucket}/products/"
    "--dwh_path"                      = "s3://${var.dwh_bucket}/dim_products/"
    "--rejected_path"                 = "s3://${var.rejected_bucket}/"
    "--enable-metrics"                = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                  = "python"
    "--TempDir"                       = "s3://${var.scripts_bucket}/temp/"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_worker_count
  timeout           = 15 # minutes

  execution_property {
    max_concurrent_runs = 1
  }
}

# ===========================================================================
# Glue ETL Job: Orders
# ===========================================================================
resource "aws_glue_job" "orders" {
  name     = "${local.prefix}-etl-orders"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/etl/jobs/etl_orders.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"              = "delta"
    "--extra-py-files"                = "s3://${var.scripts_bucket}/etl.zip"
    "--raw_path"                      = "s3://${var.raw_bucket}/orders/"
    "--dwh_path"                      = "s3://${var.dwh_bucket}/fact_orders/"
    "--rejected_path"                 = "s3://${var.rejected_bucket}/"
    "--enable-metrics"                = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                  = "python"
    "--TempDir"                       = "s3://${var.scripts_bucket}/temp/"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_worker_count
  timeout           = 15
  
  execution_property {
    max_concurrent_runs = 1
  }
}

# ===========================================================================
# Glue ETL Job: Order Items
# ===========================================================================
# This job has extra arguments for the reference table paths (orders + products)
# because it performs referential integrity checks during validation.
resource "aws_glue_job" "order_items" {
  name     = "${local.prefix}-etl-order-items"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/etl/jobs/etl_order_items.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"              = "delta"
    "--extra-py-files"                = "s3://${var.scripts_bucket}/etl.zip"
    "--raw_path"                      = "s3://${var.raw_bucket}/order_items/"
    "--dwh_path"                      = "s3://${var.dwh_bucket}/fact_order_items/"
    "--orders_dwh_path"               = "s3://${var.dwh_bucket}/fact_orders/"
    "--products_dwh_path"             = "s3://${var.dwh_bucket}/dim_products/"
    "--rejected_path"                 = "s3://${var.rejected_bucket}/"
    "--enable-metrics"                = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                  = "python"
    "--TempDir"                       = "s3://${var.scripts_bucket}/temp/"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_worker_count
  timeout           = 20 # Slightly longer — most complex job with ref integrity checks

  execution_property {
    max_concurrent_runs = 1
  }
}

# ===========================================================================
# Glue Crawler — Discovers and Catalogs Delta Tables
# ===========================================================================
# The crawler walks the DWH S3 bucket, detects the Delta table format,
# and registers/updates tables in the Glue Data Catalog. This makes the
# tables queryable through Athena without manually defining schemas.
resource "aws_glue_crawler" "dwh" {
  name          = "${local.prefix}-dwh-crawler"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.lakehouse.name

  delta_target {
    delta_tables = [
      "s3://${var.dwh_bucket}/dim_products/",
      "s3://${var.dwh_bucket}/fact_orders/",
      "s3://${var.dwh_bucket}/fact_order_items/",
    ]
    write_manifest = true
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableLevelConfiguration = 3
    }
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}
