"""
Tests for the validation rules engine.

These tests use a local SparkSession (no AWS dependencies) and verify that
each validation function correctly separates valid from rejected records.
The test fixtures use small, hand-crafted DataFrames that exercise edge cases:
null values, invalid timestamps, orphan foreign keys, and duplicates.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for the entire test session (shared across tests)."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("LakehouseTests")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestValidateNotNull:
    """Tests for the validate_not_null rule."""

    def test_all_valid_rows_pass(self, spark):
        """When no nulls are present, all rows should be in the valid set."""
        from etl.validation.rules import validate_not_null

        data = [(1, "Product A"), (2, "Product B"), (3, "Product C")]
        df = spark.createDataFrame(data, ["product_id", "product_name"])

        valid, rejected = validate_not_null(df, columns=["product_id", "product_name"])

        assert valid.count() == 3
        assert rejected.count() == 0

    def test_null_primary_key_rejected(self, spark):
        """Rows with a null primary key must be rejected."""
        from etl.validation.rules import validate_not_null

        data = [(1, "Product A"), (None, "Product B"), (3, None)]
        df = spark.createDataFrame(data, ["product_id", "product_name"])

        valid, rejected = validate_not_null(df, columns=["product_id", "product_name"])

        assert valid.count() == 1  # Only row 1 has both non-null
        assert rejected.count() == 2

    def test_rejection_reason_contains_column_names(self, spark):
        """The rejection reason should name which columns were null."""
        from etl.validation.rules import validate_not_null

        schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
        ])
        df = spark.createDataFrame([(None, "Product A")], schema)

        _, rejected = validate_not_null(
            df, columns=["product_id"], reason_prefix="test: "
        )

        reason = rejected.select("rejection_reason").first()[0]
        assert "product_id" in reason
        assert "test: " in reason


class TestValidateTimestamp:
    """Tests for the validate_timestamp_format rule."""

    def test_valid_timestamps_pass(self, spark):
        """Properly formatted timestamps should pass validation."""
        from etl.validation.rules import validate_timestamp_format

        data = [
            (1, "2025-04-01T11:27:00"),
            (2, "2025-04-01T17:53:00"),
        ]
        df = spark.createDataFrame(data, ["order_id", "order_timestamp"])

        valid, rejected = validate_timestamp_format(df, "order_timestamp")

        assert valid.count() == 2
        assert rejected.count() == 0

    def test_invalid_timestamp_rejected(self, spark):
        """Unparseable timestamp strings should be rejected."""
        from etl.validation.rules import validate_timestamp_format

        data = [
            (1, "2025-04-01T11:27:00"),
            (2, "not-a-timestamp"),
            (3, None),
        ]
        df = spark.createDataFrame(data, ["order_id", "order_timestamp"])

        valid, rejected = validate_timestamp_format(df, "order_timestamp")

        assert valid.count() == 1
        assert rejected.count() == 2


class TestValidatePositiveAmount:
    """Tests for the validate_positive_amount rule."""

    def test_positive_amounts_pass(self, spark):
        """Positive amounts should pass validation."""
        from etl.validation.rules import validate_positive_amount

        data = [(1, 229.53), (2, 131.93), (3, 0.01)]
        df = spark.createDataFrame(data, ["order_id", "total_amount"])

        valid, rejected = validate_positive_amount(df, "total_amount")

        assert valid.count() == 3
        assert rejected.count() == 0

    def test_zero_and_negative_rejected(self, spark):
        """Zero and negative amounts should be rejected."""
        from etl.validation.rules import validate_positive_amount

        data = [(1, 100.0), (2, 0.0), (3, -50.0), (4, None)]
        df = spark.createDataFrame(data, ["order_id", "total_amount"])

        valid, rejected = validate_positive_amount(df, "total_amount")

        assert valid.count() == 1  # Only the 100.0 row
        assert rejected.count() == 3


class TestValidateReferentialIntegrity:
    """Tests for the validate_referential_integrity rule."""

    def test_all_references_exist(self, spark):
        """When all foreign keys match, no records should be rejected."""
        from etl.validation.rules import validate_referential_integrity

        items = [(1, 100), (2, 101), (3, 102)]
        df = spark.createDataFrame(items, ["id", "order_id"])

        orders = [(100,), (101,), (102,)]
        ref_df = spark.createDataFrame(orders, ["order_id"])

        valid, rejected = validate_referential_integrity(
            df, ref_df, join_col="order_id"
        )

        assert valid.count() == 3
        assert rejected.count() == 0

    def test_orphan_references_rejected(self, spark):
        """Items referencing non-existent orders should be rejected."""
        from etl.validation.rules import validate_referential_integrity

        items = [(1, 100), (2, 101), (3, 999)]  # 999 doesn't exist in orders
        df = spark.createDataFrame(items, ["id", "order_id"])

        orders = [(100,), (101,)]
        ref_df = spark.createDataFrame(orders, ["order_id"])

        valid, rejected = validate_referential_integrity(
            df, ref_df, join_col="order_id"
        )

        assert valid.count() == 2
        assert rejected.count() == 1
        # Verify the orphan row is the one with order_id=999
        rejected_order_id = rejected.select("order_id").first()[0]
        assert rejected_order_id == 999


class TestDeduplicate:
    """Tests for the deduplicate function."""

    def test_no_duplicates_unchanged(self, spark):
        """A DataFrame with no duplicates should be returned unchanged."""
        from etl.validation.rules import deduplicate

        data = [(1, "A"), (2, "B"), (3, "C")]
        df = spark.createDataFrame(data, ["id", "name"])

        result = deduplicate(df, key_cols=["id"])
        assert result.count() == 3

    def test_duplicates_removed(self, spark):
        """Duplicate rows on the key column should be deduplicated."""
        from etl.validation.rules import deduplicate

        data = [(1, "A"), (1, "A_dup"), (2, "B")]
        df = spark.createDataFrame(data, ["id", "name"])

        result = deduplicate(df, key_cols=["id"])
        assert result.count() == 2

    def test_keeps_latest_when_ordered(self, spark):
        """When an order column is specified, the most recent row should be kept."""
        from etl.validation.rules import deduplicate

        data = [
            (1, "old", "2025-04-01T10:00:00"),
            (1, "new", "2025-04-01T12:00:00"),
            (2, "only", "2025-04-01T11:00:00"),
        ]
        df = spark.createDataFrame(data, ["id", "name", "ts"])

        result = deduplicate(df, key_cols=["id"], order_col="ts")
        assert result.count() == 2

        # The kept row for id=1 should be "new" (later timestamp)
        row_1 = result.filter(F.col("id") == 1).first()
        assert row_1["name"] == "new"


class TestCheckRejectRatio:
    """Tests for the check_reject_ratio safety check."""

    def test_within_threshold_passes(self):
        """A rejection ratio within the threshold should not raise."""
        from etl.validation.rules import check_reject_ratio

        # 5% rejection rate, 10% threshold — should pass
        check_reject_ratio(100, 5, 0.10, "test")

    def test_exceeds_threshold_raises(self):
        """A rejection ratio above the threshold should raise ValueError."""
        from etl.validation.rules import check_reject_ratio

        with pytest.raises(ValueError, match="exceeds threshold"):
            check_reject_ratio(100, 15, 0.10, "test")

    def test_empty_input_raises(self):
        """Zero total rows should raise ValueError (can't process nothing)."""
        from etl.validation.rules import check_reject_ratio

        with pytest.raises(ValueError, match="empty"):
            check_reject_ratio(0, 0, 0.10, "test")
