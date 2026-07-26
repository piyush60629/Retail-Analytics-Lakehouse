from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.spark_storage import (
    spark_path_exists,
)
from src.common.storage_config import (
    join_storage_path,
)


def drop_columns_if_present(
    dataframe: DataFrame,
    columns: Iterable[str],
) -> DataFrame:
    """
    Drop columns only when they exist.
    """

    existing_columns = [
        column_name
        for column_name in columns
        if column_name in dataframe.columns
    ]

    if existing_columns:
        return dataframe.drop(*existing_columns)

    return dataframe


def list_storage_directories(
    spark: SparkSession,
    root_path: str,
) -> list[str]:
    """
    List directories under a local or cloud storage path.

    Uses Spark's Hadoop filesystem API so the same logic works
    with local storage and ADLS Gen2.
    """

    root_path = str(root_path)

    hadoop_configuration = (
        spark.sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    path_object = spark._jvm.org.apache.hadoop.fs.Path(
        root_path
    )

    filesystem = path_object.getFileSystem(
        hadoop_configuration
    )

    if not filesystem.exists(path_object):
        return []

    statuses = filesystem.listStatus(
        path_object
    )

    directories = []

    for status in statuses:
        if status.isDirectory():
            directories.append(
                status.getPath().getName()
            )

    return directories


def find_previous_scd2_batch(
    spark: SparkSession,
    scd2_root: str,
    current_batch_date: str,
) -> str | None:
    """
    Find the latest SCD2 batch before the current batch.

    Compatible with local storage and ADLS Gen2.
    """

    if not spark_path_exists(
        spark=spark,
        path=scd2_root,
    ):
        return None

    available_dates = []

    directory_names = list_storage_directories(
        spark=spark,
        root_path=scd2_root,
    )

    for directory_name in directory_names:
        if not directory_name.startswith(
            "batch_date="
        ):
            continue

        batch_date = directory_name.replace(
            "batch_date=",
            "",
            1,
        )

        if batch_date < current_batch_date:
            available_dates.append(
                batch_date
            )

    if not available_dates:
        return None

    return max(available_dates)


def read_silver_dimension(
    spark: SparkSession,
    silver_root: str,
    batch_date: str,
    dataset_name: str,
) -> DataFrame:
    """
    Read a normal Silver dimension snapshot.
    """

    path = join_storage_path(
        silver_root,
        f"batch_date={batch_date}",
        dataset_name,
    )

    if not spark_path_exists(
        spark=spark,
        path=path,
    ):
        raise FileNotFoundError(
            f"Silver dataset not found: {path}"
        )

    return spark.read.parquet(
        path
    )


def read_previous_scd2_dimension(
    spark: SparkSession,
    scd2_root: str,
    batch_date: str,
    dataset_name: str,
) -> DataFrame:
    """
    Read the previous historical dimension.
    """

    path = join_storage_path(
        scd2_root,
        f"batch_date={batch_date}",
        dataset_name,
    )

    if not spark_path_exists(
        spark=spark,
        path=path,
    ):
        raise FileNotFoundError(
            f"SCD2 dimension not found: {path}"
        )

    return spark.read.parquet(
        path
    )


def read_cdc_change(
    spark: SparkSession,
    cdc_root: str,
    batch_date: str,
    dataset_name: str,
    change_type: str,
) -> DataFrame:
    """
    Read inserted, updated or deleted CDC records.
    """

    path = join_storage_path(
        cdc_root,
        f"batch_date={batch_date}",
        dataset_name,
        change_type,
    )

    if not spark_path_exists(
        spark=spark,
        path=path,
    ):
        raise FileNotFoundError(
            f"CDC path not found: {path}"
        )

    return spark.read.parquet(
        path
    )


def initialise_dimension(
    silver_dataframe: DataFrame,
    initial_batch_date: str,
) -> DataFrame:
    """
    Convert the first available Silver snapshot into an
    initial SCD Type 2 dimension.

    The initial records use a historical sentinel date so
    facts created before the pipeline's first batch can still
    resolve to a dimension version.
    """

    historical_start_date = "1900-01-01"

    return (
        silver_dataframe
        .withColumn(
            "effective_from",
            F.to_date(
                F.lit(historical_start_date)
            ),
        )
        .withColumn(
            "effective_to",
            F.lit(None).cast("date"),
        )
        .withColumn(
            "is_current",
            F.lit(True),
        )
        .withColumn(
            "record_version",
            F.lit(1).cast("int"),
        )
        .withColumn(
            "scd_initial_load_date",
            F.to_date(
                F.lit(initial_batch_date)
            ),
        )
    )


def prepare_new_records(
    dataframe: DataFrame,
    batch_date: str,
    version_column=None,
) -> DataFrame:
    """
    Add SCD Type 2 metadata to inserted or updated rows.
    """

    prepared = (
        dataframe
        .withColumn(
            "effective_from",
            F.to_date(
                F.lit(batch_date)
            ),
        )
        .withColumn(
            "effective_to",
            F.lit(None).cast("date"),
        )
        .withColumn(
            "is_current",
            F.lit(True),
        )
    )

    if version_column is None:
        prepared = prepared.withColumn(
            "record_version",
            F.lit(1).cast("int"),
        )
    else:
        prepared = prepared.withColumn(
            "record_version",
            version_column.cast("int"),
        )

    return prepared


def expire_current_records(
    historical_dimension: DataFrame,
    changed_keys: DataFrame,
    primary_keys: list[str],
    batch_date: str,
) -> DataFrame:
    """
    Expire current records matching updated or deleted keys.
    """

    key_marker = (
        changed_keys
        .select(*primary_keys)
        .dropDuplicates(primary_keys)
        .withColumn(
            "_record_changed",
            F.lit(True),
        )
    )

    joined = historical_dimension.join(
        key_marker,
        on=primary_keys,
        how="left",
    )

    expiration_date = F.date_sub(
        F.to_date(
            F.lit(batch_date)
        ),
        1,
    )

    return (
        joined
        .withColumn(
            "effective_to",
            F.when(
                F.col(
                    "_record_changed"
                ).isNotNull()
                & F.col("is_current"),
                expiration_date,
            ).otherwise(
                F.col("effective_to")
            ),
        )
        .withColumn(
            "is_current",
            F.when(
                F.col(
                    "_record_changed"
                ).isNotNull()
                & F.col("is_current"),
                F.lit(False),
            ).otherwise(
                F.col("is_current")
            ),
        )
        .drop("_record_changed")
    )


def calculate_next_versions(
    updated_records: DataFrame,
    historical_dimension: DataFrame,
    primary_keys: list[str],
) -> DataFrame:
    """
    Determine the next version number for updated records.
    """

    maximum_versions = (
        historical_dimension
        .groupBy(*primary_keys)
        .agg(
            F.max(
                "record_version"
            ).alias(
                "_maximum_record_version"
            )
        )
    )

    return (
        updated_records
        .join(
            maximum_versions,
            on=primary_keys,
            how="left",
        )
        .withColumn(
            "_next_record_version",
            F.coalesce(
                F.col(
                    "_maximum_record_version"
                ),
                F.lit(0),
            )
            + F.lit(1),
        )
        .drop(
            "_maximum_record_version"
        )
    )


def validate_single_current_record(
    dataframe: DataFrame,
    primary_keys: list[str],
    dataset_name: str,
) -> None:
    """
    Ensure each business key has no more than one current row.
    """

    duplicate_current_count = (
        dataframe
        .filter(
            F.col("is_current")
        )
        .groupBy(*primary_keys)
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_current_count > 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{duplicate_current_count} keys with more "
            f"than one current record."
        )


def validate_date_ranges(
    dataframe: DataFrame,
    dataset_name: str,
) -> None:
    """
    Ensure expired rows do not have invalid date ranges.
    """

    invalid_count = (
        dataframe
        .filter(
            F.col(
                "effective_to"
            ).isNotNull()
            & (
                F.col("effective_to")
                < F.col("effective_from")
            )
        )
        .count()
    )

    if invalid_count > 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{invalid_count} invalid SCD2 date ranges."
        )


def validate_current_end_dates(
    dataframe: DataFrame,
    dataset_name: str,
) -> None:
    """
    Current records must have a null effective_to date.
    """

    invalid_count = (
        dataframe
        .filter(
            F.col("is_current")
            & F.col(
                "effective_to"
            ).isNotNull()
        )
        .count()
    )

    if invalid_count > 0:
        raise ValueError(
            f"{dataset_name} contains current rows "
            f"with an effective_to value."
        )


def write_dimension(
    dataframe: DataFrame,
    output_root: str,
    batch_date: str,
    dataset_name: str,
) -> str:
    """
    Write an SCD Type 2 dimension snapshot.

    Compatible with local storage and ADLS Gen2.
    """

    output_path = join_storage_path(
        output_root,
        f"batch_date={batch_date}",
        dataset_name,
    )

    (
        dataframe.write
        .mode("overwrite")
        .parquet(output_path)
    )

    return output_path