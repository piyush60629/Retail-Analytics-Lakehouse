from __future__ import annotations

from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def find_previous_silver_batch(
    silver_root: Path,
    current_batch_date: str,
) -> str | None:
    """
    Find the most recent Silver batch before the current batch.
    """

    if not silver_root.exists():
        return None

    previous_dates = []

    for batch_directory in silver_root.glob(
        "batch_date=*"
    ):
        if not batch_directory.is_dir():
            continue

        batch_date = batch_directory.name.replace(
            "batch_date=",
            "",
        )

        if batch_date < current_batch_date:
            previous_dates.append(batch_date)

    if not previous_dates:
        return None

    return max(previous_dates)


def read_previous_silver(
    spark: SparkSession,
    silver_root: Path,
    batch_date: str,
    dataset_name: str,
) -> DataFrame:
    """
    Read a dataset from a previous Silver batch.
    """

    input_path = (
        silver_root
        / f"batch_date={batch_date}"
        / dataset_name
    )

    if not input_path.exists():
        raise FileNotFoundError(
            f"Previous Silver dataset not found: "
            f"{input_path}"
        )

    return spark.read.parquet(str(input_path))


def read_cdc_change(
    spark: SparkSession,
    cdc_root: Path,
    batch_date: str,
    dataset_name: str,
    change_type: str,
) -> DataFrame:
    """
    Read one CDC change category.

    change_type must be:
    inserted, updated, unchanged or deleted.
    """

    input_path = (
        cdc_root
        / f"batch_date={batch_date}"
        / dataset_name
        / change_type
    )

    if not input_path.exists():
        raise FileNotFoundError(
            f"CDC dataset not found: {input_path}"
        )

    return spark.read.parquet(str(input_path))


def drop_columns_if_present(
    dataframe: DataFrame,
    column_names: Iterable[str],
) -> DataFrame:
    """
    Drop only columns that exist in the DataFrame.
    """

    existing_columns = [
        column_name
        for column_name in column_names
        if column_name in dataframe.columns
    ]

    if existing_columns:
        dataframe = dataframe.drop(*existing_columns)

    return dataframe


def combine_key_dataframes(
    dataframes: list[DataFrame],
    primary_keys: list[str],
) -> DataFrame:
    """
    Combine primary keys from updated and deleted CDC records.
    """

    selected_dataframes = [
        dataframe.select(*primary_keys)
        for dataframe in dataframes
    ]

    combined = selected_dataframes[0]

    for dataframe in selected_dataframes[1:]:
        combined = combined.unionByName(dataframe)

    return combined.dropDuplicates(primary_keys)


def remove_changed_records(
    previous_silver: DataFrame,
    records_to_remove: DataFrame,
    primary_keys: list[str],
) -> DataFrame:
    """
    Remove old versions of updated or deleted records.
    """

    removal_keys = records_to_remove.select(
        *primary_keys
    ).dropDuplicates(primary_keys)

    return previous_silver.join(
        removal_keys,
        on=primary_keys,
        how="left_anti",
    )


def build_incremental_snapshot(
    previous_silver: DataFrame,
    transformed_upserts: DataFrame,
    updated_cdc: DataFrame,
    deleted_cdc: DataFrame,
    primary_keys: list[str],
) -> DataFrame:
    """
    Build the new Silver snapshot.

    1. Remove previous versions of updated records.
    2. Remove deleted records.
    3. Add transformed inserted and updated records.
    """

    records_to_remove = combine_key_dataframes(
        dataframes=[
            updated_cdc,
            deleted_cdc,
        ],
        primary_keys=primary_keys,
    )

    retained_records = remove_changed_records(
        previous_silver=previous_silver,
        records_to_remove=records_to_remove,
        primary_keys=primary_keys,
    )

    return retained_records.unionByName(
        transformed_upserts,
        allowMissingColumns=True,
    )


def validate_unique_primary_keys(
    dataframe: DataFrame,
    primary_keys: list[str],
    dataset_name: str,
) -> None:
    """
    Ensure the merged Silver snapshot has no duplicate keys.
    """

    duplicate_count = (
        dataframe
        .groupBy(*primary_keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{duplicate_count} duplicate primary keys "
            f"after the incremental merge."
        )


def write_silver_snapshot(
    dataframe: DataFrame,
    silver_root: Path,
    batch_date: str,
    dataset_name: str,
) -> Path:
    """
    Write the complete merged Silver snapshot.
    """

    output_path = (
        silver_root
        / f"batch_date={batch_date}"
        / dataset_name
    )

    (
        dataframe.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    return output_path