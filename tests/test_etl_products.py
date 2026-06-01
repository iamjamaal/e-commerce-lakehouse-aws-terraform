"""
Tests for the Products ETL job.

These tests verify the complete products pipeline using small in-memory
DataFrames and a temporary local directory as the Delta table path.
No AWS services are needed.
"""

import os
import shutil
import tempfile
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all products tests."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("TestProductsETL")
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
    """Create temporary directories for raw, dwh, and rejected zones."""
    dirs = {
        "raw": tempfile.mkdtemp(),
        "dwh": tempfile.mkdtemp(),
        "rejected": tempfile.mkdtemp(),
    }
    yield dirs
    for d in dirs.values():
        shutil.rmtree(d, ignore_errors=True)


class TestProductsETLIntegration:
    """Integration tests that run the full products pipeline."""

    def test_initial_load_creates_delta_table(self, spark, temp_dirs):
        """First run should create a new Delta table with all valid rows."""
        from etl.jobs.etl_products import run_products_etl

        # Write test CSV to the raw directory
        data = [
            (1, 4, "Books", "Product_1"),
            (2, 2, "Toys", "Product_2"),
            (3, 6, "Sports", "Product_3"),
        ]
        df = spark.createDataFrame(data, ["product_id", "department_id", "department", "product_name"])
        df.write.mode("overwrite").option("header", "true").csv(temp_dirs["raw"])

        metrics = run_products_etl(
            spark=spark,
            raw_path=temp_dirs["raw"],
            dwh_path=temp_dirs["dwh"],
            rejected_path=temp_dirs["rejected"],
        )

        assert metrics["rows_read"] == 3
        assert metrics["rows_valid"] == 3
        assert metrics["rows_rejected"] == 0
        assert metrics["merge_type"] == "initial_load"

        # Verify the Delta table was created and contains all 3 rows
        result = spark.read.format("delta").load(temp_dirs["dwh"])
        assert result.count() == 3

    def test_upsert_updates_existing_and_inserts_new(self, spark, temp_dirs):
        """Second run should merge: update existing products and insert new ones."""
        from etl.jobs.etl_products import run_products_etl

        # Initial load
        data_v1 = [
            (1, 4, "Books", "Product_1_Old"),
            (2, 2, "Toys", "Product_2"),
        ]
        df_v1 = spark.createDataFrame(data_v1, ["product_id", "department_id", "department", "product_name"])
        df_v1.write.mode("overwrite").option("header", "true").csv(temp_dirs["raw"])

        run_products_etl(
            spark=spark,
            raw_path=temp_dirs["raw"],
            dwh_path=temp_dirs["dwh"],
            rejected_path=temp_dirs["rejected"],
        )

        # Second load with updated product 1 and new product 3
        data_v2 = [
            (1, 4, "Books", "Product_1_Updated"),
            (3, 6, "Sports", "Product_3_New"),
        ]
        df_v2 = spark.createDataFrame(data_v2, ["product_id", "department_id", "department", "product_name"])
        # Clear and rewrite raw
        shutil.rmtree(temp_dirs["raw"])
        os.makedirs(temp_dirs["raw"])
        df_v2.write.mode("overwrite").option("header", "true").csv(temp_dirs["raw"])

        metrics = run_products_etl(
            spark=spark,
            raw_path=temp_dirs["raw"],
            dwh_path=temp_dirs["dwh"],
            rejected_path=temp_dirs["rejected"],
        )

        assert metrics["merge_type"] == "upsert"

        # Verify: product 1 updated, product 2 unchanged, product 3 new
        result = spark.read.format("delta").load(temp_dirs["dwh"])
        assert result.count() == 3

        product_1 = result.filter(F.col("product_id") == 1).first()
        assert product_1["product_name"] == "Product_1_Updated"

    def test_null_product_id_rejected(self, spark, temp_dirs):
        """Rows with null product_id should be rejected and logged."""
        from etl.jobs.etl_products import run_products_etl

        # 10 valid + 1 null = 9.1% rejection rate, below the 10% threshold
        data = [
            (1, 4, "Books", "Product_1"),
            (2, 2, "Toys", "Product_2"),
            (3, 6, "Sports", "Product_3"),
            (4, 1, "Food", "Product_4"),
            (5, 3, "Home", "Product_5"),
            (6, 5, "Beauty", "Product_6"),
            (7, 7, "Tech", "Product_7"),
            (8, 8, "Clothing", "Product_8"),
            (9, 9, "Garden", "Product_9"),
            (10, 10, "Music", "Product_10"),
            (None, 2, "Toys", "Product_Null"),  # This should be rejected
        ]
        df = spark.createDataFrame(data, ["product_id", "department_id", "department", "product_name"])
        df.write.mode("overwrite").option("header", "true").csv(temp_dirs["raw"])

        metrics = run_products_etl(
            spark=spark,
            raw_path=temp_dirs["raw"],
            dwh_path=temp_dirs["dwh"],
            rejected_path=temp_dirs["rejected"],
        )

        assert metrics["rows_read"] == 11
        assert metrics["rows_valid"] == 10
        assert metrics["rows_rejected"] == 1
        assert metrics["rows_merged"] == 10
