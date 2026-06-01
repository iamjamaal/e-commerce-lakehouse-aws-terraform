"""
Spark session factory with Delta Lake configuration.

Creates a SparkSession pre-configured for Delta Lake operations on AWS S3.
In a real AWS Glue environment, Glue provides the SparkSession automatically,
but this factory allows local testing and explicit config control.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "LakehouseETL") -> SparkSession:
    """
    Build and return a SparkSession configured for Delta Lake.

    The key configurations:
    - delta extensions: enables Delta SQL commands (MERGE, OPTIMIZE, VACUUM)
    - delta catalog:    makes Delta tables visible to Spark's catalog
    - s3a settings:     would be set in production for S3 access (via IAM roles in Glue)

    Args:
        app_name: Name for the Spark application (shows in Spark UI).

    Returns:
        A configured SparkSession instance.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.properties.defaults.logRetentionDuration", "interval 30 days")
        .master("local[*]")
    )

    # In Glue, Delta Lake is pre-configured via --datalake-formats delta.
    # Locally, load Delta JARs via spark.jars.packages and wire the extensions.
    try:
        import awsglue  # noqa: F401 — presence signals we're in Glue
        return builder.getOrCreate()
    except ImportError:
        return (
            builder
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )
