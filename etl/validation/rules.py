"""
Validation rules engine for the Lakehouse ETL pipeline.

1. Apply validation rules to the incoming DataFrame.
2. Split into 'valid' and 'rejected' DataFrames.
3. Write rejected records to the rejected zone with a reason column.
4. Only the valid records proceed to the Delta merge/upsert.

Each validation function returns a tuple of (valid_df, rejected_df) so
they can be chained together. The rejected DataFrames are tagged with a
'rejection_reason' column for debugging.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_not_null(df: DataFrame, columns: list, reason_prefix: str = "") -> tuple:
    """
    Reject any row where one or more of the specified columns is null.

    This enforces the "no null primary identifiers" rule from the project
    brief. Primary keys (product_id, order_id, id) must never be null
    because they're used as merge keys in the Delta upsert logic — a null
    merge key would cause unpredictable behavior.

    Args:
        df:             Input DataFrame to validate.
        columns:        List of column names that must not be null.
        reason_prefix:  Optional prefix for the rejection reason string.

    Returns:
        Tuple of (valid_df, rejected_df). The rejected_df includes a
        'rejection_reason' column explaining which columns were null.
    """
    # Build a condition that is True when ALL specified columns are non-null
    not_null_condition = F.lit(True)
    for col_name in columns:
        not_null_condition = not_null_condition & F.col(col_name).isNotNull()

    valid_df = df.filter(not_null_condition)

    # For rejected rows, build a descriptive reason showing which cols were null
    null_reasons = F.concat_ws(
        ", ",
        *[
            F.when(F.col(c).isNull(), F.lit(c))
            for c in columns
        ]
    )
    rejected_df = (
        df.filter(~not_null_condition)
        .withColumn(
            "rejection_reason",
            F.concat(
                F.lit(f"{reason_prefix}null_primary_key: "),
                null_reasons,
            )
        )
    )

    return valid_df, rejected_df


def validate_timestamp_format(
    df: DataFrame,
    timestamp_col: str,
    reason_prefix: str = "",
) -> tuple:
    """
    Reject rows where the timestamp column cannot be parsed as a valid timestamp.

    The source files use ISO 8601 format (e.g., '2025-04-01T11:27:00'). Rows
    where the timestamp is null, empty, or unparseable are rejected. This ensures
    downstream analytics on time-series data are reliable.

    Args:
        df:             Input DataFrame.
        timestamp_col:  Name of the timestamp column to validate.
        reason_prefix:  Optional prefix for rejection reason.

    Returns:
        Tuple of (valid_df, rejected_df).
    """
    # Attempt to cast to timestamp — invalid values become null
    df_with_check = df.withColumn(
        "_ts_check",
        F.to_timestamp(F.col(timestamp_col))
    )

    valid_df = (
        df_with_check
        .filter(F.col("_ts_check").isNotNull())
        .drop("_ts_check")
    )

    rejected_df = (
        df_with_check
        .filter(F.col("_ts_check").isNull())
        .drop("_ts_check")
        .withColumn(
            "rejection_reason",
            F.lit(f"{reason_prefix}invalid_timestamp: {timestamp_col}")
        )
    )

    return valid_df, rejected_df


def validate_positive_amount(
    df: DataFrame,
    amount_col: str,
    reason_prefix: str = "",
) -> tuple:
    """
    Reject rows where a monetary amount is null, zero, or negative.

    For orders, total_amount should always be a positive value. An order
    with zero or negative total is likely a data error and should be
    investigated rather than loaded into the analytics layer.

    Args:
        df:           Input DataFrame.
        amount_col:   Name of the amount column to validate.
        reason_prefix: Optional prefix for rejection reason.

    Returns:
        Tuple of (valid_df, rejected_df).
    """
    valid_condition = (
        F.col(amount_col).isNotNull() & (F.col(amount_col) > 0)
    )

    valid_df = df.filter(valid_condition)
    rejected_df = df.filter(~valid_condition).withColumn(
        "rejection_reason",
        F.lit(f"{reason_prefix}non_positive_amount: {amount_col}")
    )

    return valid_df, rejected_df


def validate_referential_integrity(
    df: DataFrame,
    ref_df: DataFrame,
    join_col: str,
    ref_col: str = None,
    reason_prefix: str = "",
) -> tuple:
    """
    Reject rows where a foreign key doesn't exist in the reference table.

    This enforces referential integrity between tables. For example,
    every order_id in order_items must exist in the orders table, and
    every product_id in order_items must exist in the products table.
    Orphan records would cause broken joins in analytics queries.

    The implementation uses a left-anti join to efficiently find rows
    in `df` that have no match in `ref_df`.

    Args:
        df:             The DataFrame to validate (child table).
        ref_df:         The reference DataFrame (parent table).
        join_col:       Column name in df to check.
        ref_col:        Column name in ref_df to match against. Defaults to join_col.
        reason_prefix:  Optional prefix for rejection reason.

    Returns:
        Tuple of (valid_df, rejected_df).
    """
    if ref_col is None:
        ref_col = join_col

    # Select only the reference column to minimize shuffle data
    ref_keys = ref_df.select(F.col(ref_col).alias(join_col)).distinct()

    # Left semi join: keep only rows that DO have a match in the reference
    valid_df = df.join(ref_keys, on=join_col, how="left_semi")

    # Left anti join: keep only rows that DON'T have a match (orphans)
    rejected_df = (
        df.join(ref_keys, on=join_col, how="left_anti")
        .withColumn(
            "rejection_reason",
            F.concat(
                F.lit(f"{reason_prefix}referential_integrity_violation: "),
                F.lit(f"{join_col} not found in reference table"),
            )
        )
    )

    return valid_df, rejected_df


def deduplicate(df: DataFrame, key_cols: list, order_col: str = None) -> DataFrame:
    """
    Remove duplicate rows based on key columns.

    When duplicates exist, we keep the most recent record (determined by
    order_col). If no order_col is specified, we keep an arbitrary row —
    which is fine for idempotent re-ingestion where all duplicates are
    identical.

    This is critical for the merge/upsert pattern: if the incoming batch
    itself contains duplicates on the merge key, the Delta MERGE would fail
    with "multiple source rows matched the same target row."

    Args:
        df:         Input DataFrame.
        key_cols:   Columns that define uniqueness.
        order_col:  Optional column to determine recency (keeps latest).

    Returns:
        Deduplicated DataFrame.
    """
    if order_col:
        # Use a window function to rank rows within each key group,
        # keeping the most recent (highest order_col value)
        from pyspark.sql.window import Window

        window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
        return (
            df.withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )
    else:
        return df.dropDuplicates(key_cols)


def check_reject_ratio(
    total_count: int,
    rejected_count: int,
    max_ratio: float,
    dataset_name: str,
) -> None:
    """
    Safety check: fail the job if too many records are being rejected.

    This prevents a bad source file from silently dropping most of its data.
    For example, if a CSV is malformed and 50% of rows fail validation,
    we'd rather fail loudly than load a half-empty table.

    Args:
        total_count:    Total number of incoming rows.
        rejected_count: Number of rows that failed validation.
        max_ratio:      Maximum allowed rejection ratio (e.g., 0.10 for 10%).
        dataset_name:   Name of the dataset (for error messages).

    Raises:
        ValueError: If the rejection ratio exceeds max_ratio.
    """
    if total_count == 0:
        raise ValueError(f"[{dataset_name}] Incoming data is empty — nothing to process.")

    ratio = rejected_count / total_count
    if ratio > max_ratio:
        raise ValueError(
            f"[{dataset_name}] Rejection ratio {ratio:.1%} exceeds threshold "
            f"{max_ratio:.1%}. Rejected {rejected_count} of {total_count} rows. "
            f"Job aborted — investigate source data quality."
        )
