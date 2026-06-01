"""
Tests for the Order Items ETL job.
"""

import shutil
import tempfile
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all order items tests."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("TestOrderItemsETL")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def temp_dirs():
    """Create isolated temp directories for each test."""
    dirs = {
        "raw": tempfile.mkdtemp(),
        "dwh": tempfile.mkdtemp(),
        "orders_dwh": tempfile.mkdtemp(),
        "products_dwh": tempfile.mkdtemp(),
        "rejected": tempfile.mkdtemp(),
    }
    yield dirs
    for d in dirs.values():
        shutil.rmtree(d, ignore_errors=True)


def _make_order_items_df(spark, rows):
    """Helper: build an order_items DataFrame from tuples."""
    return spark.createDataFrame(
        rows,
        [
            "id", "order_id", "user_id", "days_since_prior_order",
            "product_id", "add_to_cart_order", "reordered",
            "order_timestamp", "date",
        ],
    )


def _seed_orders_delta(spark, path, order_ids):
    """
    Helper: create a minimal orders Delta table for referential integrity tests.
    Only the order_id column matters for the ref check.
    """
    data = [(oid, 1, 1, "2025-04-01T10:00:00", 100.0, "2025-04-01") for oid in order_ids]
    df = spark.createDataFrame(
        data,
        ["order_id", "order_num", "user_id", "order_timestamp", "total_amount", "date"],
    )
    df.write.format("delta").mode("overwrite").save(path)


def _seed_products_delta(spark, path, product_ids):
    """
    Helper: create a minimal products Delta table for referential integrity tests.
    Only the product_id column matters for the ref check.
    """
    data = [(pid, 1, "Dept", f"Product_{pid}") for pid in product_ids]
    df = spark.createDataFrame(
        data,
        ["product_id", "department_id", "department", "product_name"],
    )
    df.write.format("delta").mode("overwrite").save(path)


class TestOrderItemsETLIntegration:
    """End-to-end integration tests for the order items pipeline."""

    def test_initial_load_all_valid(self, spark, temp_dirs):
        """
        When all rows are valid and reference tables exist with matching keys,
        every row should land in the Delta table on the first run.
        """
        from etl.jobs.etl_order_items import run_order_items_etl

        # Seed reference tables with IDs that our test rows will reference
        _seed_orders_delta(spark, temp_dirs["orders_dwh"], [10000, 10001])
        _seed_products_delta(spark, temp_dirs["products_dwh"], [501, 502, 503])

        input_data = [
            (1, 10000, 100, 7, 501, 1, 0, "2025-04-01T11:27:00", "2025-04-01"),
            (2, 10000, 100, 7, 502, 2, 1, "2025-04-01T11:27:00", "2025-04-01"),
            (3, 10001, 200, 3, 503, 1, 0, "2025-04-01T17:53:00", "2025-04-01"),
        ]
        df = _make_order_items_df(spark, input_data)

        metrics = run_order_items_etl(
            spark=spark,
            dwh_path=temp_dirs["dwh"],
            orders_dwh_path=temp_dirs["orders_dwh"],
            products_dwh_path=temp_dirs["products_dwh"],
            rejected_path=temp_dirs["rejected"],
            input_df=df,
        )

        assert metrics["rows_read"] == 3
        assert metrics["rows_valid"] == 3
        assert metrics["rows_rejected"] == 0
        assert metrics["merge_type"] == "initial_load"
        assert metrics["rows_merged"] == 3

        # Verify the Delta table contents
        result = spark.read.format("delta").load(temp_dirs["dwh"])
        assert result.count() == 3

    def test_orphan_order_id_rejected(self, spark, temp_dirs):
        """
        An order_item referencing an order_id that doesn't exist in the
        orders table should be rejected with a referential integrity violation.
        """
        from etl.jobs.etl_order_items import run_order_items_etl

        # Orders table only has order_id=10000
        _seed_orders_delta(spark, temp_dirs["orders_dwh"], [10000])
        _seed_products_delta(spark, temp_dirs["products_dwh"], [501, 502])

        # 10 valid + 1 orphan = 9.1% rejection, below the 10% threshold
        input_data = [
            (i, 10000, 100, 7, 501, i, 0, "2025-04-01T11:00:00", "2025-04-01")
            for i in range(1, 11)
        ] + [
            (11, 99999, 200, 3, 502, 1, 0, "2025-04-01T12:00:00", "2025-04-01"),  # Orphan order_id
        ]
        df = _make_order_items_df(spark, input_data)

        metrics = run_order_items_etl(
            spark=spark,
            dwh_path=temp_dirs["dwh"],
            orders_dwh_path=temp_dirs["orders_dwh"],
            products_dwh_path=temp_dirs["products_dwh"],
            rejected_path=temp_dirs["rejected"],
            input_df=df,
        )

        assert metrics["rows_valid"] == 10
        assert metrics["rows_rejected"] == 1
        assert metrics["rows_merged"] == 10

    def test_orphan_product_id_rejected(self, spark, temp_dirs):
        """
        An order_item referencing a product_id that doesn't exist in the
        products table should be rejected.
        """
        from etl.jobs.etl_order_items import run_order_items_etl

        _seed_orders_delta(spark, temp_dirs["orders_dwh"], [10000])
        # Products table only has product_id=501
        _seed_products_delta(spark, temp_dirs["products_dwh"], [501])

        # 10 valid + 1 orphan = 9.1% rejection, below the 10% threshold
        input_data = [
            (i, 10000, 100, 7, 501, i, 0, "2025-04-01T11:00:00", "2025-04-01")
            for i in range(1, 11)
        ] + [
            (11, 10000, 100, 7, 888, 2, 1, "2025-04-01T11:00:00", "2025-04-01"),  # 888 not in products
        ]
        df = _make_order_items_df(spark, input_data)

        metrics = run_order_items_etl(
            spark=spark,
            dwh_path=temp_dirs["dwh"],
            orders_dwh_path=temp_dirs["orders_dwh"],
            products_dwh_path=temp_dirs["products_dwh"],
            rejected_path=temp_dirs["rejected"],
            input_df=df,
        )

        assert metrics["rows_valid"] == 10
        assert metrics["rows_rejected"] == 1

    def test_null_primary_key_rejected(self, spark, temp_dirs):
        """Rows with null id, order_id, or product_id should be rejected."""
        from etl.jobs.etl_order_items import run_order_items_etl

        _seed_orders_delta(spark, temp_dirs["orders_dwh"], [10000])
        _seed_products_delta(spark, temp_dirs["products_dwh"], [501])

        # 10 valid + 1 null = 9.1% rejection rate, below the 10% threshold
        input_data = [
            (i, 10000, 100, 7, 501, i, 0, "2025-04-01T11:00:00", "2025-04-01")
            for i in range(1, 11)
        ] + [
            (None, 10000, 100, 7, 501, 2, 0, "2025-04-01T11:00:00", "2025-04-01"),  # Null id
        ]
        df = _make_order_items_df(spark, input_data)

        metrics = run_order_items_etl(
            spark=spark,
            dwh_path=temp_dirs["dwh"],
            orders_dwh_path=temp_dirs["orders_dwh"],
            products_dwh_path=temp_dirs["products_dwh"],
            rejected_path=temp_dirs["rejected"],
            input_df=df,
        )

        assert metrics["rows_valid"] == 10
        assert metrics["rows_rejected"] == 1

    def test_graceful_without_reference_tables(self, spark, temp_dirs):
        """
        When reference tables (orders, products) don't exist yet — as happens
        during the very first pipeline run — the job should still succeed by
        skipping the referential integrity checks. This graceful degradation
        is critical because Step Functions runs all three jobs in parallel,
        so order_items might execute before orders/products are ready.
        """
        from etl.jobs.etl_order_items import run_order_items_etl

        input_data = [
            (1, 10000, 100, 7, 501, 1, 0, "2025-04-01T11:00:00", "2025-04-01"),
            (2, 10001, 200, 3, 502, 1, 0, "2025-04-01T12:00:00", "2025-04-01"),
        ]
        df = _make_order_items_df(spark, input_data)

        # Pass non-existent paths — the job should handle this gracefully
        metrics = run_order_items_etl(
            spark=spark,
            dwh_path=temp_dirs["dwh"],
            orders_dwh_path="/tmp/nonexistent_orders_delta",
            products_dwh_path="/tmp/nonexistent_products_delta",
            rejected_path=temp_dirs["rejected"],
            input_df=df,
        )

        # Both rows should pass since ref checks were skipped
        assert metrics["rows_valid"] == 2
        assert metrics["rows_rejected"] == 0
        assert metrics["rows_merged"] == 2

    def test_upsert_updates_existing_rows(self, spark, temp_dirs):
        """
        A second run with updated data for an existing id should update
        the row in the Delta table (not duplicate it).
        """
        from etl.jobs.etl_order_items import run_order_items_etl

        _seed_orders_delta(spark, temp_dirs["orders_dwh"], [10000])
        _seed_products_delta(spark, temp_dirs["products_dwh"], [501])

        # Initial load
        v1_data = [
            (1, 10000, 100, 7, 501, 1, 0, "2025-04-01T10:00:00", "2025-04-01"),
        ]
        df_v1 = _make_order_items_df(spark, v1_data)

        run_order_items_etl(
            spark=spark,
            dwh_path=temp_dirs["dwh"],
            orders_dwh_path=temp_dirs["orders_dwh"],
            products_dwh_path=temp_dirs["products_dwh"],
            rejected_path=temp_dirs["rejected"],
            input_df=df_v1,
        )

        # Second load — same id=1 but updated add_to_cart_order
        v2_data = [
            (1, 10000, 100, 7, 501, 5, 1, "2025-04-01T14:00:00", "2025-04-01"),
        ]
        df_v2 = _make_order_items_df(spark, v2_data)

        metrics = run_order_items_etl(
            spark=spark,
            dwh_path=temp_dirs["dwh"],
            orders_dwh_path=temp_dirs["orders_dwh"],
            products_dwh_path=temp_dirs["products_dwh"],
            rejected_path=temp_dirs["rejected"],
            input_df=df_v2,
        )

        assert metrics["merge_type"] == "upsert"

        result = spark.read.format("delta").load(temp_dirs["dwh"])
        assert result.count() == 1  # Still just 1 row, not 2
        row = result.first()
        assert row["add_to_cart_order"] == 5
        assert row["reordered"] == 1

    def test_duplicate_ids_in_batch_deduplicated(self, spark, temp_dirs):
        """
        If the incoming batch contains two rows with the same id, the
        deduplication step should keep only the latest (by timestamp).
        """
        from etl.jobs.etl_order_items import run_order_items_etl

        _seed_orders_delta(spark, temp_dirs["orders_dwh"], [10000])
        _seed_products_delta(spark, temp_dirs["products_dwh"], [501])

        input_data = [
            (1, 10000, 100, 7, 501, 1, 0, "2025-04-01T10:00:00", "2025-04-01"),
            (1, 10000, 100, 7, 501, 3, 1, "2025-04-01T15:00:00", "2025-04-01"),  # Same id, later ts
        ]
        df = _make_order_items_df(spark, input_data)

        metrics = run_order_items_etl(
            spark=spark,
            dwh_path=temp_dirs["dwh"],
            orders_dwh_path=temp_dirs["orders_dwh"],
            products_dwh_path=temp_dirs["products_dwh"],
            rejected_path=temp_dirs["rejected"],
            input_df=df,
        )

        assert metrics["rows_read"] == 2
        assert metrics["rows_merged"] == 1  # Deduplicated down to 1

        result = spark.read.format("delta").load(temp_dirs["dwh"])
        assert result.count() == 1
        row = result.first()
        assert row["add_to_cart_order"] == 3  # The later version
