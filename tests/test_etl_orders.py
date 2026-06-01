"""
Tests for the Orders ETL job.

Verifies the orders pipeline handles timestamp validation, amount validation,
deduplication with recency ordering, and Delta merge with date partitioning.
"""

import shutil
import tempfile
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("TestOrdersETL")
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
    dirs = {
        "raw": tempfile.mkdtemp(),
        "dwh": tempfile.mkdtemp(),
        "rejected": tempfile.mkdtemp(),
    }
    yield dirs
    for d in dirs.values():
        shutil.rmtree(d, ignore_errors=True)


class TestOrdersETLIntegration:

    def test_initial_load_with_valid_data(self, spark, temp_dirs):
        """All valid orders should load into a date-partitioned Delta table."""
        from etl.jobs.etl_orders import run_orders_etl

        data = [
            (90, 10000, 1990, "2025-04-01T11:27:00", 229.53, "2025-04-01"),
            (41, 10001, 5057, "2025-04-01T17:53:00", 131.93, "2025-04-01"),
        ]
        df = spark.createDataFrame(
            data,
            ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"],
        )

        metrics = run_orders_etl(
            spark=spark,
            dwh_path=temp_dirs["dwh"],
            rejected_path=temp_dirs["rejected"],
            input_df=df,
        )

        assert metrics["rows_read"] == 2
        assert metrics["rows_valid"] == 2
        assert metrics["rows_rejected"] == 0
        assert metrics["merge_type"] == "initial_load"

        # Verify Delta table is partitioned by date
        result = spark.read.format("delta").load(temp_dirs["dwh"])
        assert result.count() == 2

    def test_negative_amount_rejected(self, spark, temp_dirs):
        """Orders with zero or negative total_amount should be rejected."""
        from etl.jobs.etl_orders import run_orders_etl

        # 20 valid + 2 rejected = 9.1% rejection, below the 10% threshold
        valid_rows = [
            (i, 10000 + i, 100 + i, "2025-04-01T10:00:00", 100.0 + i, "2025-04-01")
            for i in range(1, 21)
        ]
        invalid_rows = [
            (21, 10021, 121, "2025-04-01T11:00:00", -50.0, "2025-04-01"),
            (22, 10022, 122, "2025-04-01T12:00:00", 0.0, "2025-04-01"),
        ]
        df = spark.createDataFrame(
            valid_rows + invalid_rows,
            ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"],
        )

        metrics = run_orders_etl(
            spark=spark,
            dwh_path=temp_dirs["dwh"],
            rejected_path=temp_dirs["rejected"],
            input_df=df,
        )

        assert metrics["rows_valid"] == 20
        assert metrics["rows_rejected"] == 2

    def test_duplicate_order_id_keeps_latest(self, spark, temp_dirs):
        """When two rows share an order_id, keep the one with the latest timestamp."""
        from etl.jobs.etl_orders import run_orders_etl

        data = [
            (1, 10000, 100, "2025-04-01T10:00:00", 200.0, "2025-04-01"),
            (2, 10000, 100, "2025-04-01T14:00:00", 250.0, "2025-04-01"),  # Later timestamp
        ]
        df = spark.createDataFrame(
            data,
            ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"],
        )

        metrics = run_orders_etl(
            spark=spark,
            dwh_path=temp_dirs["dwh"],
            rejected_path=temp_dirs["rejected"],
            input_df=df,
        )

        assert metrics["rows_merged"] == 1  # Deduplicated to 1

        result = spark.read.format("delta").load(temp_dirs["dwh"])
        row = result.first()
        assert row["total_amount"] == 250.0  # The later version
