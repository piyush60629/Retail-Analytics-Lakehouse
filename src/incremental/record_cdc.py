from __future__ import annotations
import os
import sys

from src.common.storage_config import LOCAL_DATA_ROOT, join_storage_path

import json
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ingestion.schemas import (
    DATASET_PRIMARY_KEYS,
)
from src.incremental.config import (
    BRONZE_ROOT,
    CDC_ROOT,
)


DATASETS = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]

def spark_path_exists(
    spark: SparkSession,
    storage_path: str,
) -> bool:
    """
    Check whether a local or Hadoop-compatible storage path exists.
    """

    hadoop_configuration = (
        spark.sparkContext._jsc.hadoopConfiguration()
    )

    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(
        storage_path
    )

    filesystem = hadoop_path.getFileSystem(
        hadoop_configuration
    )

    return filesystem.exists(hadoop_path)


def create_spark_session() -> SparkSession:
    """
    Create a Spark session for record-level CDC.

    Spark workers use the Python executable from the active
    virtual environment.
    """

    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    spark = (
        SparkSession.builder
        .appName("RetailAnalyticsRecordCDC")
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
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "8",
        )
        .config(
            "spark.driver.memory",
            "2g",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def extract_batch_date(
    batch_directory_name: str,
) -> str:
    """
    Extract YYYY-MM-DD from batch_date=YYYY-MM-DD.
    """

    return batch_directory_name.replace(
        "batch_date=",
        "",
    )


def _get_hadoop_path_and_filesystem(
    spark: SparkSession,
    storage_path: str,
):
    """
    Return Hadoop Path and FileSystem objects.
    """

    hadoop_configuration = (
        spark.sparkContext._jsc.hadoopConfiguration()
    )

    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(
        storage_path
    )

    filesystem = hadoop_path.getFileSystem(
        hadoop_configuration
    )

    return hadoop_path, filesystem


def delete_storage_path_if_exists(
    spark: SparkSession,
    storage_path: str,
) -> None:
    """
    Recursively delete a local or cloud path if present.
    """

    hadoop_path, filesystem = (
        _get_hadoop_path_and_filesystem(
            spark=spark,
            storage_path=storage_path,
        )
    )

    if filesystem.exists(hadoop_path):
        if not filesystem.delete(hadoop_path, True):
            raise RuntimeError(
                f"Unable to delete path: {storage_path}"
            )


def write_json_file(
    spark: SparkSession,
    output_path: str,
    payload: dict[str, Any],
) -> None:
    """
    Write a JSON audit file using Hadoop FileSystem.
    """

    hadoop_path, filesystem = (
        _get_hadoop_path_and_filesystem(
            spark=spark,
            storage_path=output_path,
        )
    )

    parent_path = hadoop_path.getParent()

    if (
        parent_path is not None
        and not filesystem.exists(parent_path)
    ):
        if not filesystem.mkdirs(parent_path):
            raise RuntimeError(
                f"Unable to create directory: "
                f"{parent_path}"
            )

    output_stream = filesystem.create(
        hadoop_path,
        True,
    )

    try:
        output_stream.write(
            bytearray(
                json.dumps(
                    payload,
                    indent=4,
                    default=str,
                ).encode("utf-8")
            )
        )
    finally:
        output_stream.close()


def find_previous_batch_date(
    spark: SparkSession,
    current_batch_date: str,
) -> str | None:
    """
    Find the latest Bronze batch older than the current batch.
    """

    if not spark_path_exists(
        spark,
        BRONZE_ROOT,
    ):
        return None

    batch_glob_path = join_storage_path(
        BRONZE_ROOT,
        "batch_date=*",
    )

    hadoop_glob_path, filesystem = (
        _get_hadoop_path_and_filesystem(
            spark=spark,
            storage_path=batch_glob_path,
        )
    )

    batch_statuses = filesystem.globStatus(
        hadoop_glob_path
    )

    if batch_statuses is None:
        return None

    available_dates: list[str] = []

    for batch_status in batch_statuses:
        if not batch_status.isDirectory():
            continue

        batch_directory_name = (
            batch_status
            .getPath()
            .getName()
        )

        batch_date = extract_batch_date(
            batch_directory_name
        )

        if batch_date < current_batch_date:
            available_dates.append(batch_date)

    if not available_dates:
        return None

    return max(available_dates)


def read_bronze_dataset(
    spark: SparkSession,
    batch_date: str,
    dataset_name: str,
) -> DataFrame:
    """
    Read one Bronze dataset for a batch.
    """

    dataset_path = join_storage_path(
        BRONZE_ROOT,
        f"batch_date={batch_date}",
        dataset_name,
    )

    if not spark_path_exists(
        spark,
        dataset_path,
    ):
        raise FileNotFoundError(
            f"Bronze dataset not found: {dataset_path}"
        )

    # Prefer the validated "valid" output so quarantined rows
    # never flow into CDC, Silver or Gold. Fall back to raw
    # Bronze for batches that were never validated.
    valid_path = join_storage_path(
        LOCAL_DATA_ROOT,
        "validated",
        f"batch_date={batch_date}",
        dataset_name,
        "valid",
    )

    if spark_path_exists(spark, valid_path):
        bronze_columns = spark.read.parquet(dataset_path).columns
        return spark.read.parquet(valid_path).select(*bronze_columns)

    return spark.read.parquet(dataset_path)


def build_primary_key_condition(
    current_alias: str,
    previous_alias: str,
    primary_keys: list[str],
):
    """
    Build a join condition using one or more primary keys.
    """

    condition = None

    for primary_key in primary_keys:
        current_condition = (
            F.col(
                f"{current_alias}.{primary_key}"
            )
            ==
            F.col(
                f"{previous_alias}.{primary_key}"
            )
        )

        condition = (
            current_condition
            if condition is None
            else condition & current_condition
        )

    return condition


def select_current_columns(
    dataframe: DataFrame,
    alias_name: str,
) -> list:
    """
    Select all columns from the current dataset.
    """

    return [
        F.col(f"{alias_name}.{column_name}").alias(
            column_name
        )
        for column_name in dataframe.columns
    ]


def add_change_metadata(
    dataframe: DataFrame,
    change_type: str,
    current_batch_date: str,
    previous_batch_date: str | None,
) -> DataFrame:
    """
    Add CDC technical metadata.
    """

    return (
        dataframe
        .withColumn(
            "change_type",
            F.lit(change_type),
        )
        .withColumn(
            "cdc_batch_date",
            F.to_date(
                F.lit(current_batch_date)
            ),
        )
        .withColumn(
            "compared_with_batch_date",
            F.to_date(
                F.lit(previous_batch_date)
            )
            if previous_batch_date
            else F.lit(None).cast("date"),
        )
        .withColumn(
            "cdc_timestamp",
            F.current_timestamp(),
        )
    )


def detect_first_batch_changes(
    current_dataframe: DataFrame,
    current_batch_date: str,
) -> dict[str, DataFrame]:
    """
    Treat all records as inserted when no previous batch exists.
    """

    inserted = add_change_metadata(
        dataframe=current_dataframe,
        change_type="INSERTED",
        current_batch_date=current_batch_date,
        previous_batch_date=None,
    )

    empty_dataframe = (
        inserted
        .limit(0)
    )

    return {
        "inserted": inserted,
        "updated": empty_dataframe,
        "unchanged": empty_dataframe,
        "deleted": empty_dataframe,
    }


def detect_dataset_changes(
    current_dataframe: DataFrame,
    previous_dataframe: DataFrame,
    primary_keys: list[str],
    current_batch_date: str,
    previous_batch_date: str,
) -> dict[str, DataFrame]:
    """
    Detect inserted, updated, unchanged and deleted records.
    """

    current_alias = "current"
    previous_alias = "previous"

    current = current_dataframe.alias(
        current_alias
    )

    previous = previous_dataframe.alias(
        previous_alias
    )

    join_condition = build_primary_key_condition(
        current_alias=current_alias,
        previous_alias=previous_alias,
        primary_keys=primary_keys,
    )

    joined = current.join(
        previous,
        on=join_condition,
        how="full_outer",
    )

    first_primary_key = primary_keys[0]

    current_columns = select_current_columns(
        dataframe=current_dataframe,
        alias_name=current_alias,
    )

    previous_columns = select_current_columns(
        dataframe=previous_dataframe,
        alias_name=previous_alias,
    )

    inserted = (
        joined
        .filter(
            F.col(
                f"{previous_alias}.{first_primary_key}"
            ).isNull()
            &
            F.col(
                f"{current_alias}.{first_primary_key}"
            ).isNotNull()
        )
        .select(*current_columns)
    )

    inserted = add_change_metadata(
        dataframe=inserted,
        change_type="INSERTED",
        current_batch_date=current_batch_date,
        previous_batch_date=previous_batch_date,
    )

    updated = (
        joined
        .filter(
            F.col(
                f"{current_alias}.{first_primary_key}"
            ).isNotNull()
            &
            F.col(
                f"{previous_alias}.{first_primary_key}"
            ).isNotNull()
            &
            (
                F.col(
                    f"{current_alias}.record_hash"
                )
                !=
                F.col(
                    f"{previous_alias}.record_hash"
                )
            )
        )
        .select(*current_columns)
    )

    updated = add_change_metadata(
        dataframe=updated,
        change_type="UPDATED",
        current_batch_date=current_batch_date,
        previous_batch_date=previous_batch_date,
    )

    unchanged = (
        joined
        .filter(
            F.col(
                f"{current_alias}.{first_primary_key}"
            ).isNotNull()
            &
            F.col(
                f"{previous_alias}.{first_primary_key}"
            ).isNotNull()
            &
            (
                F.col(
                    f"{current_alias}.record_hash"
                )
                ==
                F.col(
                    f"{previous_alias}.record_hash"
                )
            )
        )
        .select(*current_columns)
    )

    unchanged = add_change_metadata(
        dataframe=unchanged,
        change_type="UNCHANGED",
        current_batch_date=current_batch_date,
        previous_batch_date=previous_batch_date,
    )

    deleted = (
        joined
        .filter(
            F.col(
                f"{current_alias}.{first_primary_key}"
            ).isNull()
            &
            F.col(
                f"{previous_alias}.{first_primary_key}"
            ).isNotNull()
        )
        .select(*previous_columns)
    )

    deleted = add_change_metadata(
        dataframe=deleted,
        change_type="DELETED",
        current_batch_date=current_batch_date,
        previous_batch_date=previous_batch_date,
    )

    return {
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "deleted": deleted,
    }


def write_change_dataset(
    dataframe: DataFrame,
    output_path: str,
) -> None:
    """
    Write one CDC result as Parquet.
    """

    (
        dataframe.write
        .mode("overwrite")
        .parquet(output_path)
    )


def build_audit_record(
    dataset_name: str,
    current_count: int,
    previous_count: int,
    inserted_count: int,
    updated_count: int,
    unchanged_count: int,
    deleted_count: int,
) -> dict[str, Any]:
    """
    Create a CDC audit record.
    """

    current_reconciliation = (
        inserted_count
        + updated_count
        + unchanged_count
    )

    previous_reconciliation = (
        updated_count
        + unchanged_count
        + deleted_count
    )

    return {
        "dataset_name": dataset_name,
        "current_record_count": current_count,
        "previous_record_count": previous_count,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "deleted_count": deleted_count,
        "current_count_reconciliation": (
            current_reconciliation
            == current_count
        ),
        "previous_count_reconciliation": (
            previous_reconciliation
            == previous_count
        ),
        "audit_timestamp": datetime.now().isoformat(),
    }


def process_dataset_cdc(
    spark: SparkSession,
    dataset_name: str,
    current_batch_date: str,
    previous_batch_date: str | None,
    output_root: str,
) -> dict[str, Any]:
    """
    Run record-level CDC for one dataset.
    """

    current_dataframe = read_bronze_dataset(
        spark=spark,
        batch_date=current_batch_date,
        dataset_name=dataset_name,
    )

    current_count = current_dataframe.count()

    if previous_batch_date is None:
        changes = detect_first_batch_changes(
            current_dataframe=current_dataframe,
            current_batch_date=current_batch_date,
        )

        previous_count = 0

    else:
        previous_dataframe = read_bronze_dataset(
            spark=spark,
            batch_date=previous_batch_date,
            dataset_name=dataset_name,
        )

        previous_count = previous_dataframe.count()

        changes = detect_dataset_changes(
            current_dataframe=current_dataframe,
            previous_dataframe=previous_dataframe,
            primary_keys=DATASET_PRIMARY_KEYS[
                dataset_name
            ],
            current_batch_date=current_batch_date,
            previous_batch_date=previous_batch_date,
        )

    dataset_output_root = join_storage_path(
        output_root,
        dataset_name,
    )

    counts = {}

    for change_name, dataframe in changes.items():
        change_output_path = join_storage_path(
            dataset_output_root,
            change_name,
        )

        write_change_dataset(
            dataframe=dataframe,
            output_path=change_output_path,
        )

        counts[change_name] = dataframe.count()

    audit_record = build_audit_record(
        dataset_name=dataset_name,
        current_count=current_count,
        previous_count=previous_count,
        inserted_count=counts["inserted"],
        updated_count=counts["updated"],
        unchanged_count=counts["unchanged"],
        deleted_count=counts["deleted"],
    )

    if not audit_record[
        "current_count_reconciliation"
    ]:
        raise RuntimeError(
            f"Current count reconciliation failed "
            f"for {dataset_name}."
        )

    if (
        previous_batch_date is not None
        and not audit_record[
            "previous_count_reconciliation"
        ]
    ):
        raise RuntimeError(
            f"Previous count reconciliation failed "
            f"for {dataset_name}."
        )

    print(
        f"{dataset_name:<12} | "
        f"Inserted: {counts['inserted']:>8,} | "
        f"Updated: {counts['updated']:>8,} | "
        f"Unchanged: {counts['unchanged']:>8,} | "
        f"Deleted: {counts['deleted']:>8,}"
    )

    return audit_record


def run_record_level_cdc(
    current_batch_date: str,
) -> None:
    """
    Run record-level CDC for all Bronze datasets.
    """

    spark = create_spark_session()

    try:
        current_bronze_path = join_storage_path(
            BRONZE_ROOT,
            f"batch_date={current_batch_date}",
        )

        if not spark_path_exists(
            spark,
            current_bronze_path,
        ):
            raise FileNotFoundError(
                f"Current Bronze batch not found: "
                f"{current_bronze_path}"
            )

        previous_batch_date = find_previous_batch_date(
            spark=spark,
            current_batch_date=current_batch_date,
        )

        output_root = join_storage_path(
            CDC_ROOT,
            f"batch_date={current_batch_date}",
        )

        delete_storage_path_if_exists(
            spark=spark,
            storage_path=output_root,
        )

        audit_records = []

        print("\n" + "=" * 110)
        print(
            f"RECORD-LEVEL CDC — CURRENT BATCH: "
            f"{current_batch_date}"
        )
        print(
            f"PREVIOUS BATCH: "
            f"{previous_batch_date or 'NONE — FIRST BATCH'}"
        )
        print("=" * 110)

        for dataset_name in DATASETS:
            audit_record = process_dataset_cdc(
                spark=spark,
                dataset_name=dataset_name,
                current_batch_date=current_batch_date,
                previous_batch_date=previous_batch_date,
                output_root=output_root,
            )

            audit_records.append(audit_record)

        audit_path = join_storage_path(
            output_root,
            f"cdc_audit_{current_batch_date}.json",
        )

        write_json_file(
            spark=spark,
            output_path=audit_path,
            payload={
                "current_batch_date": current_batch_date,
                "previous_batch_date": previous_batch_date,
                "datasets": audit_records,
            },
        )

        print("\n" + "=" * 110)
        print("RECORD-LEVEL CDC COMPLETED")
        print(f"CDC location: {output_root}")
        print(f"Audit report: {audit_path}")
        print("=" * 110)

    finally:
        spark.stop()

