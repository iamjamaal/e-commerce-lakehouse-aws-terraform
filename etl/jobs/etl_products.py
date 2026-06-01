"""
ETL Job: Products (Dimension Table)

This job ingests the products CSV from the S3 raw zone, validates it,
deduplicates it, and merges it into a Delta Lake dimension table using
SCD Type 1 (overwrite on change) logic.

Pipeline steps:
    1. READ    — Load CSV from the raw zone
    2. VALIDATE — Enforce non-null product_id, non-null product_name
    3. DEDUP   — Remove duplicates on product_id within the incoming batch
    4. MERGE   — Upsert into the Delta table (insert new, update changed)
    5. ARCHIVE — Move the raw file to the archive zone
"""

import sys
sys.path.insert(0, ".")  # Must precede local imports when run outside Glue

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from config.settings import (  # noqa: E402
    RAW_PRODUCTS_PATH,
    DWH_PRODUCTS_PATH,
    REJECTED_PATH,
    PRODUCTS_MERGE_KEY,
    MAX_REJECT_RATIO,
)
from etl.utils.spark_session import get_spark_session  # noqa: E402
from etl.utils.s3_utils import read_csv_from_s3, write_delta_table  # noqa: E402
from etl.utils.logger import get_logger, log_step, log_job_start, log_job_end  # noqa: E402
from etl.validation.rules import (  # noqa: E402
    validate_not_null,
    deduplicate,
    check_reject_ratio,
)


def run_products_etl(
    spark=None,
    raw_path: str = RAW_PRODUCTS_PATH,
    dwh_path: str = DWH_PRODUCTS_PATH,
    rejected_path: str = REJECTED_PATH,
):
    """
    Execute the full products ETL pipeline.

    Args:
        spark:         Optional SparkSession (created if not provided).
        raw_path:      Override for the raw zone path (useful in tests).
        dwh_path:      Override for the DWH Delta table path.
        rejected_path: Override for the rejected records path.

    Returns:
        dict with pipeline metrics (rows_read, rows_valid, rows_rejected, rows_merged).
    """
    logger = get_logger("etl_products")
    log_job_start(logger, "products")

    if spark is None:
        spark = get_spark_session("ETL_Products")

    metrics = {}

    # ── Step 1: READ ─────────────────────────────────────────────────────
    log_step(logger, "READ", f"Reading products from {raw_path}")
    df_raw = read_csv_from_s3(spark, raw_path)
    metrics["rows_read"] = df_raw.count()
    log_step(logger, "READ", f"Loaded {metrics['rows_read']} rows")

    # ── Step 2: CLEAN ────────────────────────────────────────────────────
    # Strip whitespace from string columns and normalize nulls
    log_step(logger, "CLEAN", "Trimming whitespace and normalizing empty strings")
    df_clean = df_raw
    for col_name in ["department", "product_name"]:
        df_clean = df_clean.withColumn(col_name, F.trim(F.col(col_name)))
        df_clean = df_clean.withColumn(
            col_name,
            F.when(F.col(col_name) == "", None).otherwise(F.col(col_name))
        )

    # Cast product_id and department_id to integer (in case they came in as strings)
    df_clean = (
        df_clean
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("department_id", F.col("department_id").cast("int"))
    )

    # ── Step 3: VALIDATE ─────────────────────────────────────────────────
    log_step(logger, "VALIDATE", "Enforcing non-null product_id and product_name")

    df_valid, df_rejected_nulls = validate_not_null(
        df_clean,
        columns=["product_id", "product_name"],
        reason_prefix="products: ",
    )

    # Collect all rejected records
    all_rejected = df_rejected_nulls
    metrics["rows_rejected"] = all_rejected.count()
    metrics["rows_valid"] = df_valid.count()

    log_step(
        logger, "VALIDATE",
        f"{metrics['rows_valid']} passed, {metrics['rows_rejected']} rejected"
    )

    # Safety check: abort if too many rows are rejected
    check_reject_ratio(
        metrics["rows_read"], metrics["rows_rejected"],
        MAX_REJECT_RATIO, "products"
    )

    # Write rejected records for investigation
    if metrics["rows_rejected"] > 0:
        log_step(logger, "REJECT", f"Writing {metrics['rows_rejected']} rejected rows")
        all_rejected.write.mode("append").parquet(f"{rejected_path}products/")

    # ── Step 4: DEDUPLICATE ──────────────────────────────────────────────
    log_step(logger, "DEDUP", f"Deduplicating on {PRODUCTS_MERGE_KEY}")
    df_deduped = deduplicate(df_valid, key_cols=[PRODUCTS_MERGE_KEY])

    dedup_count = df_deduped.count()
    dupes_removed = metrics["rows_valid"] - dedup_count
    if dupes_removed > 0:
        log_step(logger, "DEDUP", f"Removed {dupes_removed} duplicates")

    # ── Step 5: MERGE (Upsert into Delta) ────────────────────────────────
    log_step(logger, "MERGE", f"Upserting into Delta table at {dwh_path}")

    try:
        # If the Delta table already exists, perform a MERGE (upsert)
        delta_table = DeltaTable.forPath(spark, dwh_path)

        (
            delta_table.alias("target")
            .merge(
                df_deduped.alias("source"),
                f"target.{PRODUCTS_MERGE_KEY} = source.{PRODUCTS_MERGE_KEY}"
            )
            # When a product already exists, update all its attributes (SCD Type 1)
            .whenMatchedUpdateAll()
            # When a product is new, insert it
            .whenNotMatchedInsertAll()
            .execute()
        )
        metrics["merge_type"] = "upsert"

    except Exception:
        # Table doesn't exist yet — create it with an initial full write
        log_step(logger, "MERGE", "Delta table not found — creating with initial load")
        write_delta_table(df_deduped, dwh_path, mode="overwrite")
        metrics["merge_type"] = "initial_load"

    metrics["rows_merged"] = dedup_count
    log_step(logger, "MERGE", f"Merged {metrics['rows_merged']} rows ({metrics['merge_type']})")

    log_job_end(logger, "products")
    return metrics


# ── Glue job entry point ─────────────────────────────────────────────────
# When AWS Glue runs this script, it calls the module directly.
# The if-main block ensures it also works for local testing.
if __name__ == "__main__":
    from awsglue.utils import getResolvedOptions
    _args = getResolvedOptions(sys.argv, ["raw_path", "dwh_path", "rejected_path"])
    result = run_products_etl(
        raw_path=_args["raw_path"],
        dwh_path=_args["dwh_path"],
        rejected_path=_args["rejected_path"],
    )
    print(f"Products ETL complete: {result}")
