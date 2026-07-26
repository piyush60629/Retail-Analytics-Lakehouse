from __future__ import annotations

from pyspark.sql import SparkSession


def spark_path_exists(
    spark: SparkSession,
    path: str,
) -> bool:
    """
    Check whether a local or cloud path exists
    through Spark's Hadoop filesystem.
    """

    java_path = (
        spark._jvm
        .org.apache.hadoop.fs.Path(path)
    )

    file_system = (
        java_path.getFileSystem(
            spark._jsc.hadoopConfiguration()
        )
    )

    return bool(
        file_system.exists(java_path)
    )