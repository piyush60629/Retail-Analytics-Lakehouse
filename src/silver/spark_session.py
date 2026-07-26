import os
import sys

from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """
    Create the Spark session used by the Silver pipeline.

    sys.executable ensures Spark workers use the Python interpreter
    from the currently activated virtual environment.
    """

    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    return (
        SparkSession.builder
        .appName("RetailAnalyticsLakehouse-Silver")
        .master("local[*]")
        .config(
            "spark.pyspark.python",
            python_executable,
        )
        .config(
            "spark.pyspark.driver.python",
            python_executable,
        )
        .config(
            "spark.sql.legacy.timeParserPolicy",
            "CORRECTED",
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "4",
        )
        .getOrCreate()
    )