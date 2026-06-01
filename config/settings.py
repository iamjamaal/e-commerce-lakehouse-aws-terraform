"""
Centralized configuration for the Lakehouse ETL pipeline.
All S3 paths, Delta Lake settings, schema definitions, and validation
thresholds live here so every ETL job and test references a single source of truth.
"""


# S3 bucket paths — swap these per environment (dev / staging / prod)
S3_RAW_BUCKET = "s3://lakehouse-raw"
S3_DWH_BUCKET = "s3://lakehouse-dwh"
S3_ARCHIVED_BUCKET = "s3://lakehouse-archived"
S3_REJECTED_BUCKET = "s3://lakehouse-rejected"

# Raw zone landing paths (where new files are dropped)
RAW_PRODUCTS_PATH = f"{S3_RAW_BUCKET}/products/"
RAW_ORDERS_PATH = f"{S3_RAW_BUCKET}/orders/"
RAW_ORDER_ITEMS_PATH = f"{S3_RAW_BUCKET}/order_items/"

# Delta Lake table paths in the DWH zone
DWH_PRODUCTS_PATH = f"{S3_DWH_BUCKET}/dim_products/"
DWH_ORDERS_PATH = f"{S3_DWH_BUCKET}/fact_orders/"
DWH_ORDER_ITEMS_PATH = f"{S3_DWH_BUCKET}/fact_order_items/"

# Archive destinations (raw files moved here after successful ingestion)
ARCHIVED_PRODUCTS_PATH = f"{S3_ARCHIVED_BUCKET}/products/"
ARCHIVED_ORDERS_PATH = f"{S3_ARCHIVED_BUCKET}/orders/"
ARCHIVED_ORDER_ITEMS_PATH = f"{S3_ARCHIVED_BUCKET}/order_items/"

# Rejected records destinations (records failing validation land here)
REJECTED_PATH = f"{S3_REJECTED_BUCKET}/"


# Glue Data Catalog
GLUE_DATABASE_NAME = "lakehouse_dwh"
GLUE_PRODUCTS_TABLE = "dim_products"
GLUE_ORDERS_TABLE = "fact_orders"
GLUE_ORDER_ITEMS_TABLE = "fact_order_items"


# Schema definitions — column names and expected types
# These drive both schema enforcement on read and Delta table creation.
PRODUCTS_SCHEMA = {
    "product_id": "int",
    "department_id": "int",
    "department": "string",
    "product_name": "string",
}

ORDERS_SCHEMA = {
    "order_num": "int",
    "order_id": "int",
    "user_id": "int",
    "order_timestamp": "timestamp",
    "total_amount": "double",
    "date": "date",
}

ORDER_ITEMS_SCHEMA = {
    "id": "int",
    "order_id": "int",
    "user_id": "int",
    "days_since_prior_order": "int",
    "product_id": "int",
    "add_to_cart_order": "int",
    "reordered": "int",
    "order_timestamp": "timestamp",
    "date": "date",
}

# Merge / upsert keys — the columns used in Delta MERGE ON conditions
PRODUCTS_MERGE_KEY = "product_id"
ORDERS_MERGE_KEY = "order_id"
ORDER_ITEMS_MERGE_KEY = "id"


# Partitioning — which column each fact table is partitioned by in Delta
ORDERS_PARTITION_COL = "date"
ORDER_ITEMS_PARTITION_COL = "date"
# Products is a slowly-changing dimension — no date partitioning needed.


# Validation thresholds
# If more than this fraction of incoming rows are rejected, fail the job
# rather than silently dropping too much data. Safety net for bad source files.
MAX_REJECT_RATIO = 0.10  # 10%
