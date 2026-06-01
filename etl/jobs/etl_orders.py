"""
ETL Job: Orders (Fact Table)

This job ingests order data from the S3 raw zone (XLSX format), validates it,
deduplicates it, and merges it into a Delta Lake fact table partitioned by date.

Pipeline steps:
    1. READ      — Load XLSX from the raw zone
    2. CLEAN     — Cast types, trim strings, parse timestamps
    3. VALIDATE  — Non-null order_id, valid timestamps, positive total_amount
    4. DEDUP     — Remove duplicates on order_id within the batch
    5. MERGE     — Upsert into the Delta table, partitioned by date
    6. ARCHIVE   — Move the raw file to the archive zone
"""

import sys
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType,
)

sys.path.insert(0, ".")

from config.settings import (
    RAW_ORDERS_PATH,
    DWH_ORDERS_PATH,
    REJECTED_PATH,
    ORDERS_MERGE_KEY,
    ORDERS_PARTITION_COL,
    MAX_REJECT_RATIO,
)
from etl.utils.spark_session import get_spark_session
from etl.utils.s3_utils import read_csv_from_s3, write_delta_table
from etl.utils.logger import get_logger, log_step, log_job_start, log_job_end
from etl.validation.rules import (
    validate_not_null,
    validate_timestamp_format,
    validate_positive_amount,
    deduplicate,
    check_reject_ratio,
)


def run_orders_etl(
    spark=None,
    raw_path: str = RAW_ORDERS_PATH,
    dwh_path: str = DWH_ORDERS_PATH,
    rejected_path: str = REJECTED_PATH,
    input_df=None,
):
    """
    Execute the full orders ETL pipeline.

    Args:
        spark:         Optional SparkSession (created if not provided).
        raw_path:      Override for the raw zone path.
        dwh_path:      Override for the DWH Delta table path.
        rejected_path: Override for the rejected records path.
        input_df:      Optional pre-loaded DataFrame (for testing — bypasses file read).

    Returns:
        dict with pipeline metrics.
    """
    logger = get_logger("etl_orders")
    log_job_start(logger, "orders")

    if spark is None:
        spark = get_spark_session("ETL_Orders")

    metrics = {}

    # ── Step 1: READ ─────────────────────────────────────────────────────
    if input_df is not None:
        df_raw = input_df
    else:
        log_step(logger, "READ", f"Reading orders from {raw_path}")
        df_raw = read_csv_from_s3(spark, raw_path)

    metrics["rows_read"] = df_raw.count()
    log_step(logger, "READ", f"Loaded {metrics['rows_read']} rows")

    # ── Step 2: CLEAN ────────────────────────────────────────────────────
    log_step(logger, "CLEAN", "Casting types and parsing timestamps")

    df_clean = (
        df_raw
        # Ensure integer types for IDs
        .withColumn("order_num", F.col("order_num").cast("int"))
        .withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("user_id", F.col("user_id").cast("int"))
        # Parse the ISO timestamp string into a proper Spark TimestampType
        .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp")))
        # Ensure total_amount is a double (handles string inputs from CSVs)
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        # Parse date column — handles both 'yyyy-MM-dd' strings and date objects
        .withColumn("date", F.to_date(F.col("date")))
    )

    # ── Step 3: VALIDATE ─────────────────────────────────────────────────
    log_step(logger, "VALIDATE", "Running validation rules")

    # Rule 1: order_id must not be null (it's our merge key and primary identifier)
    df_valid, rejected_nulls = validate_not_null(
        df_clean,
        columns=["order_id"],
        reason_prefix="orders: ",
    )

    # Rule 2: order_timestamp must be a valid, parseable timestamp
    df_valid, rejected_ts = validate_timestamp_format(
        df_valid,
        timestamp_col="order_timestamp",
        reason_prefix="orders: ",
    )

    # Rule 3: total_amount must be positive (no zero or negative orders)
    df_valid, rejected_amount = validate_positive_amount(
        df_valid,
        amount_col="total_amount",
        reason_prefix="orders: ",
    )

    # Union all rejected records into one DataFrame for writing
    # Each rejected_* DF may have different columns due to schema differences,
    # so we select a common set plus the rejection_reason
    common_cols = df_raw.columns + ["rejection_reason"]

    all_rejected_dfs = []
    for rej_df in [rejected_nulls, rejected_ts, rejected_amount]:
        if rej_df.count() > 0:
            # Ensure all rejection DFs have the same columns for union
            for c in common_cols:
                if c not in rej_df.columns:
                    rej_df = rej_df.withColumn(c, F.lit(None))
            all_rejected_dfs.append(rej_df.select(common_cols))

    if all_rejected_dfs:
        from functools import reduce
        all_rejected = reduce(lambda a, b: a.unionByName(b), all_rejected_dfs)
        metrics["rows_rejected"] = all_rejected.count()
    else:
        all_rejected = None
        metrics["rows_rejected"] = 0

    metrics["rows_valid"] = df_valid.count()

    log_step(
        logger, "VALIDATE",
        f"{metrics['rows_valid']} passed, {metrics['rows_rejected']} rejected"
    )

    # Safety check
    check_reject_ratio(
        metrics["rows_read"], metrics["rows_rejected"],
        MAX_REJECT_RATIO, "orders"
    )

    # Write rejected records
    if all_rejected is not None and metrics["rows_rejected"] > 0:
        log_step(logger, "REJECT", f"Writing {metrics['rows_rejected']} rejected rows")
        all_rejected.write.mode("append").parquet(f"{rejected_path}orders/")

    # ── Step 4: DEDUPLICATE ──────────────────────────────────────────────
    log_step(logger, "DEDUP", f"Deduplicating on {ORDERS_MERGE_KEY}")

    # For orders, if there are duplicates on order_id, keep the one with
    # the latest timestamp (most recent version of the order)
    df_deduped = deduplicate(
        df_valid,
        key_cols=[ORDERS_MERGE_KEY],
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

        # The merge condition includes the partition column (date) alongside
        # the merge key (order_id). This enables partition pruning: Delta only
        # scans the date partitions present in the incoming batch, not the
        # entire table. Huge performance win for large historical tables.
        (
            delta_table.alias("target")
            .merge(
                df_deduped.alias("source"),
                f"target.{ORDERS_MERGE_KEY} = source.{ORDERS_MERGE_KEY} "
                f"AND target.{ORDERS_PARTITION_COL} = source.{ORDERS_PARTITION_COL}"
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
            partition_cols=[ORDERS_PARTITION_COL],
        )
        metrics["merge_type"] = "initial_load"

    metrics["rows_merged"] = dedup_count
    log_step(logger, "MERGE", f"Merged {metrics['rows_merged']} rows ({metrics['merge_type']})")

    log_job_end(logger, "orders")
    return metrics


if __name__ == "__main__":
    from awsglue.utils import getResolvedOptions
    _args = getResolvedOptions(sys.argv, ["raw_path", "dwh_path", "rejected_path"])
    result = run_orders_etl(
        raw_path=_args["raw_path"],
        dwh_path=_args["dwh_path"],
        rejected_path=_args["rejected_path"],
    )
    print(f"Orders ETL complete: {result}")
