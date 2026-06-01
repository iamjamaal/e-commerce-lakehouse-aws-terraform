"""
ETL Job: Order Items (Fact Table)

This job ingests order line items from the S3 raw zone (XLSX format), validates
them (including referential integrity against both orders and products), deduplicates,
and merges into a Delta Lake fact table partitioned by date.
orphan items).

Pipeline steps:
    1. READ      — Load XLSX from the raw zone
    2. CLEAN     — Cast types, parse timestamps
    3. VALIDATE  — Non-null id, valid timestamps, referential integrity
    4. DEDUP     — Remove duplicates on id within the batch
    5. MERGE     — Upsert into the Delta table, partitioned by date
    6. ARCHIVE   — Move the raw file to the archive zone
"""

import sys
sys.path.insert(0, ".")  # Must precede local imports when run outside Glue

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from config.settings import (  # noqa: E402
    RAW_ORDER_ITEMS_PATH,
    DWH_ORDER_ITEMS_PATH,
    DWH_ORDERS_PATH,
    DWH_PRODUCTS_PATH,
    REJECTED_PATH,
    ORDER_ITEMS_MERGE_KEY,
    ORDER_ITEMS_PARTITION_COL,
    MAX_REJECT_RATIO,
)
from etl.utils.spark_session import get_spark_session  # noqa: E402
from etl.utils.s3_utils import read_csv_from_s3, write_delta_table  # noqa: E402
from etl.utils.logger import get_logger, log_step, log_job_start, log_job_end  # noqa: E402
from etl.validation.rules import (  # noqa: E402
    validate_not_null,
    validate_timestamp_format,
    validate_referential_integrity,
    deduplicate,
    check_reject_ratio,
)


def run_order_items_etl(
    spark=None,
    raw_path: str = RAW_ORDER_ITEMS_PATH,
    dwh_path: str = DWH_ORDER_ITEMS_PATH,
    orders_dwh_path: str = DWH_ORDERS_PATH,
    products_dwh_path: str = DWH_PRODUCTS_PATH,
    rejected_path: str = REJECTED_PATH,
    input_df=None,
    orders_ref_df=None,
    products_ref_df=None,
):
    """
    Execute the full order items ETL pipeline.

    Args:
        spark:             Optional SparkSession.
        raw_path:          Override for the raw zone path.
        dwh_path:          Override for the DWH Delta table path.
        orders_dwh_path:   Path to the orders Delta table (for referential integrity).
        products_dwh_path: Path to the products Delta table (for referential integrity).
        rejected_path:     Override for the rejected records path.
        input_df:          Optional pre-loaded DataFrame (for testing).
        orders_ref_df:     Optional pre-loaded orders reference DataFrame (for testing).
        products_ref_df:   Optional pre-loaded products reference DataFrame (for testing).

    Returns:
        dict with pipeline metrics.
    """
    logger = get_logger("etl_order_items")
    log_job_start(logger, "order_items")

    if spark is None:
        spark = get_spark_session("ETL_OrderItems")

    metrics = {}

    # ── Step 1: READ ─────────────────────────────────────────────────────
    if input_df is not None:
        df_raw = input_df
    else:
        log_step(logger, "READ", f"Reading order items from {raw_path}")
        df_raw = read_csv_from_s3(spark, raw_path)

    metrics["rows_read"] = df_raw.count()
    log_step(logger, "READ", f"Loaded {metrics['rows_read']} rows")

    # ── Step 2: CLEAN ────────────────────────────────────────────────────
    log_step(logger, "CLEAN", "Casting types and parsing timestamps")

    df_clean = (
        df_raw
        .withColumn("id", F.col("id").cast("int"))
        .withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("user_id", F.col("user_id").cast("int"))
        .withColumn("days_since_prior_order", F.col("days_since_prior_order").cast("int"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("add_to_cart_order", F.col("add_to_cart_order").cast("int"))
        .withColumn("reordered", F.col("reordered").cast("int"))
        .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp")))
        .withColumn("date", F.to_date(F.col("date")))
    )

    # ── Step 3: VALIDATE ─────────────────────────────────────────────────
    log_step(logger, "VALIDATE", "Running validation rules")

    all_rejected_dfs = []

    # Rule 1: id (primary key) must not be null
    df_valid, rejected_nulls = validate_not_null(
        df_clean,
        columns=["id", "order_id", "product_id"],
        reason_prefix="order_items: ",
    )
    if rejected_nulls.count() > 0:
        all_rejected_dfs.append(rejected_nulls)

    # Rule 2: order_timestamp must be valid
    df_valid, rejected_ts = validate_timestamp_format(
        df_valid,
        timestamp_col="order_timestamp",
        reason_prefix="order_items: ",
    )
    if rejected_ts.count() > 0:
        all_rejected_dfs.append(rejected_ts)

    # Rule 3: Referential integrity — order_id must exist in the orders table
    # Load the orders reference data (either from Delta table or test fixture)
    if orders_ref_df is None:
        try:
            orders_ref_df = spark.read.format("delta").load(orders_dwh_path)
        except Exception:
            log_step(
                logger, "VALIDATE",
                "WARNING: Orders Delta table not found — skipping order_id ref check. "
                "This is expected during initial load when orders hasn't been processed yet."
            )
            orders_ref_df = None

    if orders_ref_df is not None:
        log_step(logger, "VALIDATE", "Checking referential integrity: order_id → orders")
        df_valid, rejected_order_ref = validate_referential_integrity(
            df_valid,
            ref_df=orders_ref_df,
            join_col="order_id",
            reason_prefix="order_items: ",
        )
        if rejected_order_ref.count() > 0:
            all_rejected_dfs.append(rejected_order_ref)

    # Rule 4: Referential integrity — product_id must exist in the products table
    if products_ref_df is None:
        try:
            products_ref_df = spark.read.format("delta").load(products_dwh_path)
        except Exception:
            log_step(
                logger, "VALIDATE",
                "WARNING: Products Delta table not found — skipping product_id ref check."
            )
            products_ref_df = None

    if products_ref_df is not None:
        log_step(logger, "VALIDATE", "Checking referential integrity: product_id → products")
        df_valid, rejected_prod_ref = validate_referential_integrity(
            df_valid,
            ref_df=products_ref_df,
            join_col="product_id",
            reason_prefix="order_items: ",
        )
        if rejected_prod_ref.count() > 0:
            all_rejected_dfs.append(rejected_prod_ref)

    # Combine all rejected records
    if all_rejected_dfs:
        from functools import reduce
        # Align schemas before union — add missing columns as null
        all_cols = set()
        for rdf in all_rejected_dfs:
            all_cols.update(rdf.columns)
        all_cols = sorted(all_cols)

        aligned_dfs = []
        for rdf in all_rejected_dfs:
            for c in all_cols:
                if c not in rdf.columns:
                    rdf = rdf.withColumn(c, F.lit(None))
            aligned_dfs.append(rdf.select(all_cols))

        all_rejected = reduce(lambda a, b: a.unionByName(b), aligned_dfs)
        metrics["rows_rejected"] = all_rejected.count()
    else:
        all_rejected = None
        metrics["rows_rejected"] = 0

    metrics["rows_valid"] = df_valid.count()

    log_step(
        logger, "VALIDATE",
        f"{metrics['rows_valid']} passed, {metrics['rows_rejected']} rejected"
    )

    check_reject_ratio(
        metrics["rows_read"], metrics["rows_rejected"],
        MAX_REJECT_RATIO, "order_items"
    )

    if all_rejected is not None and metrics["rows_rejected"] > 0:
        log_step(logger, "REJECT", f"Writing {metrics['rows_rejected']} rejected rows")
        all_rejected.write.mode("append").parquet(f"{rejected_path}order_items/")

    # ── Step 4: DEDUPLICATE ──────────────────────────────────────────────
    log_step(logger, "DEDUP", f"Deduplicating on {ORDER_ITEMS_MERGE_KEY}")

    df_deduped = deduplicate(
        df_valid,
        key_cols=[ORDER_ITEMS_MERGE_KEY],
        order_col="order_timestamp",
    )

    dedup_count = df_deduped.count()
    dupes_removed = metrics["rows_valid"] - dedup_count
    if dupes_removed > 0:
        log_step(logger, "DEDUP", f"Removed {dupes_removed} duplicates")

    # ── Step 5: MERGE (Upsert into Delta, partitioned by date) ───────────
    log_step(logger, "MERGE", f"Upserting into Delta table at {dwh_path}")

    try:
        delta_table = DeltaTable.forPath(spark, dwh_path)

        (
            delta_table.alias("target")
            .merge(
                df_deduped.alias("source"),
                f"target.{ORDER_ITEMS_MERGE_KEY} = source.{ORDER_ITEMS_MERGE_KEY} "
                f"AND target.{ORDER_ITEMS_PARTITION_COL} = source.{ORDER_ITEMS_PARTITION_COL}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        metrics["merge_type"] = "upsert"

    except Exception:
        log_step(logger, "MERGE", "Delta table not found — creating with initial load")
        write_delta_table(
            df_deduped, dwh_path,
            mode="overwrite",
            partition_cols=[ORDER_ITEMS_PARTITION_COL],
        )
        metrics["merge_type"] = "initial_load"

    metrics["rows_merged"] = dedup_count
    log_step(logger, "MERGE", f"Merged {metrics['rows_merged']} rows ({metrics['merge_type']})")

    log_job_end(logger, "order_items")
    return metrics


if __name__ == "__main__":
    from awsglue.utils import getResolvedOptions
    _args = getResolvedOptions(
        sys.argv,
        ["raw_path", "dwh_path", "orders_dwh_path", "products_dwh_path", "rejected_path"],
    )
    result = run_order_items_etl(
        raw_path=_args["raw_path"],
        dwh_path=_args["dwh_path"],
        orders_dwh_path=_args["orders_dwh_path"],
        products_dwh_path=_args["products_dwh_path"],
        rejected_path=_args["rejected_path"],
    )
    print(f"Order Items ETL complete: {result}")
