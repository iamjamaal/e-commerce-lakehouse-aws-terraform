"""
S3 utility functions for the Lakehouse ETL pipeline.

Handles reading raw files from S3 (CSV and XLSX), writing Delta tables,
and archiving raw files after successful ingestion. In production, these
would use boto3 for the archive/move operations and Spark's native S3A
connector for reads/writes. For local testing, they operate on local paths.
"""

import os
import shutil
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


def read_csv_from_s3(
    spark: SparkSession,
    path: str,
    schema: StructType = None,
) -> DataFrame:
    """
    Read a CSV file (or directory of CSVs) from S3 into a Spark DataFrame.

    Args:
        spark:  Active SparkSession.
        path:   S3 path (s3://bucket/prefix/) or local path for testing.
        schema: Optional StructType to enforce on read. If None, Spark infers.

    Returns:
        DataFrame with the CSV contents.
    """
    reader = spark.read.option("header", "true").option("inferSchema", "true")

    if schema:
        reader = reader.schema(schema)

    return reader.csv(path)


def read_excel_from_s3(
    spark: SparkSession,
    path: str,
    sheet_name: str = "Sheet1",
) -> DataFrame:
    """
    Read an Excel file from S3 into a Spark DataFrame.

    In production on AWS Glue, we'd use the spark-excel library or convert
    to CSV/Parquet as a pre-processing step. For this implementation, we
    read with pandas first (since openpyxl is available), then convert to
    a Spark DataFrame.

    Args:
        spark:      Active SparkSession.
        path:       S3 path or local path to the .xlsx file.
        sheet_name: Which sheet to read (default: Sheet1).

    Returns:
        DataFrame with the Excel contents.
    """
    import pandas as pd

    # pandas can read from S3 if boto3 is configured, or from local paths
    pdf = pd.read_excel(path, sheet_name=sheet_name)
    return spark.createDataFrame(pdf)


def write_delta_table(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_cols: list = None,
) -> None:
    """
    Write a DataFrame as a Delta table to the specified path.

    This is used for the initial table creation. For incremental loads,
    use the merge/upsert logic in each ETL job instead.

    Args:
        df:             DataFrame to write.
        path:           Target S3/local path for the Delta table.
        mode:           Write mode — 'overwrite' for full refresh, 'append' for additive.
        partition_cols: List of column names to partition by (e.g., ['date']).
    """
    writer = df.write.format("delta").mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(path)


def archive_raw_file(
    source_path: str,
    archive_base_path: str,
    dataset_name: str,
) -> str:
    """
    Move a raw file from the landing zone to the archive zone.

    In production, this would use boto3's S3 copy + delete (S3 doesn't have
    a native move operation). The archive path includes a date prefix so
    files are organized chronologically.

    Args:
        source_path:       Path to the raw file to archive.
        archive_base_path: Base archive path (e.g., s3://lakehouse-archived/products/).
        dataset_name:      Name of the dataset (for logging).

    Returns:
        The destination path where the file was archived.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    filename = os.path.basename(source_path)
    dest_path = os.path.join(archive_base_path, today, filename)

    # In production: boto3 s3 copy then delete
    # For local testing: use shutil
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    shutil.copy2(source_path, dest_path)

    print(f"[ARCHIVE] {dataset_name}: {source_path} → {dest_path}")
    return dest_path
