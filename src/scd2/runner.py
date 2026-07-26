from __future__ import annotations

import json

from src.common.spark_storage import (
    spark_path_exists,
)

from src.common.storage_config import (
    join_storage_path,
)

from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.ingestion.schemas import DATASET_PRIMARY_KEYS
from src.scd2.config import (
    CDC_METADATA_COLUMNS,
    CDC_ROOT,
    DIMENSION_DATASETS,
    SCD2_AUDIT_ROOT,
    SCD2_ROOT,
    SILVER_ROOT,
)
from src.scd2.merge import (
    calculate_next_versions,
    drop_columns_if_present,
    expire_current_records,
    find_previous_scd2_batch,
    initialise_dimension,
    prepare_new_records,
    read_cdc_change,
    read_previous_scd2_dimension,
    read_silver_dimension,
    validate_current_end_dates,
    validate_date_ranges,
    validate_single_current_record,
    write_dimension,
)
from src.silver.spark_session import create_spark_session
from src.silver.transformers import TRANSFORMERS


def combine_changed_keys(
    updated_dataframe: DataFrame,
    deleted_dataframe: DataFrame,
    primary_keys: list[str],
) -> DataFrame:
    """
    Combine updated and deleted business keys.
    """

    return (
        updated_dataframe
        .select(*primary_keys)
        .unionByName(
            deleted_dataframe.select(*primary_keys)
        )
        .dropDuplicates(primary_keys)
    )


def transform_cdc_records(
    dataset_name: str,
    dataframe: DataFrame,
    batch_date: str,
) -> DataFrame:
    """
    Apply the existing Silver transformation to CDC rows.
    """

    clean_dataframe = drop_columns_if_present(
        dataframe=dataframe,
        columns=CDC_METADATA_COLUMNS,
    )

    transformer = TRANSFORMERS[dataset_name]

    return transformer(
        clean_dataframe,
        batch_date,
    )


def initialise_scd2_batch(
    spark,
    initial_batch_date: str,
) -> None:
    """
    Seed customers and products from an existing Silver batch.
    """

    audit_records = []

    print("=" * 110)
    print(
        f"SCD TYPE 2 INITIALISATION — "
        f"BATCH DATE: {initial_batch_date}"
    )
    print("=" * 110)

    for dataset_name in DIMENSION_DATASETS:
        silver_dataframe = read_silver_dimension(
            spark=spark,
            silver_root=SILVER_ROOT,
            batch_date=initial_batch_date,
            dataset_name=dataset_name,
        )

        dimension_dataframe = initialise_dimension(
            silver_dataframe=silver_dataframe,
            initial_batch_date=initial_batch_date,
        ).cache()

        record_count = dimension_dataframe.count()

        validate_single_current_record(
            dataframe=dimension_dataframe,
            primary_keys=DATASET_PRIMARY_KEYS[
                dataset_name
            ],
            dataset_name=dataset_name,
        )

        output_path = write_dimension(
            dataframe=dimension_dataframe,
            output_root=SCD2_ROOT,
            batch_date=initial_batch_date,
            dataset_name=dataset_name,
        )

        audit_records.append(
            {
                "dataset_name": dataset_name,
                "batch_date": initial_batch_date,
                "operation": "INITIALISE",
                "record_count": record_count,
                "current_record_count": record_count,
                "expired_record_count": 0,
                "output_path": str(output_path),
                "status": "SUCCESS",
            }
        )

        print(
            f"{dataset_name:<12} | "
            f"Initial records: {record_count:>8,} | "
            f"Current: {record_count:>8,}"
        )

        dimension_dataframe.unpersist()

    write_audit(
        spark=spark,
        audit_records=audit_records,
        batch_date=initial_batch_date,
    )


def process_dimension(
    spark,
    dataset_name: str,
    batch_date: str,
    previous_batch_date: str,
) -> dict[str, Any]:
    """
    Apply inserted, updated and deleted CDC changes.
    """

    primary_keys = DATASET_PRIMARY_KEYS[
        dataset_name
    ]

    previous_dimension = (
        read_previous_scd2_dimension(
            spark=spark,
            scd2_root=SCD2_ROOT,
            batch_date=previous_batch_date,
            dataset_name=dataset_name,
        )
        .cache()
    )

    inserted_cdc = (
        read_cdc_change(
            spark=spark,
            cdc_root=CDC_ROOT,
            batch_date=batch_date,
            dataset_name=dataset_name,
            change_type="inserted",
        )
        .cache()
    )

    updated_cdc = (
        read_cdc_change(
            spark=spark,
            cdc_root=CDC_ROOT,
            batch_date=batch_date,
            dataset_name=dataset_name,
            change_type="updated",
        )
        .cache()
    )

    deleted_cdc = (
        read_cdc_change(
            spark=spark,
            cdc_root=CDC_ROOT,
            batch_date=batch_date,
            dataset_name=dataset_name,
            change_type="deleted",
        )
        .cache()
    )

    inserted_count = inserted_cdc.count()
    updated_count = updated_cdc.count()
    deleted_count = deleted_cdc.count()
    previous_count = previous_dimension.count()

    changed_keys = combine_changed_keys(
        updated_dataframe=updated_cdc,
        deleted_dataframe=deleted_cdc,
        primary_keys=primary_keys,
    )

    expired_history = expire_current_records(
        historical_dimension=previous_dimension,
        changed_keys=changed_keys,
        primary_keys=primary_keys,
        batch_date=batch_date,
    )

    transformed_inserted = transform_cdc_records(
        dataset_name=dataset_name,
        dataframe=inserted_cdc,
        batch_date=batch_date,
    )

    new_inserted_versions = prepare_new_records(
        dataframe=transformed_inserted,
        batch_date=batch_date,
    )

    transformed_updated = transform_cdc_records(
        dataset_name=dataset_name,
        dataframe=updated_cdc,
        batch_date=batch_date,
    )

    updated_with_versions = calculate_next_versions(
        updated_records=transformed_updated,
        historical_dimension=previous_dimension,
        primary_keys=primary_keys,
    )

    new_updated_versions = prepare_new_records(
        dataframe=updated_with_versions,
        batch_date=batch_date,
        version_column=F.col(
            "_next_record_version"
        ),
    ).drop("_next_record_version")

    final_dimension = (
        expired_history
        .unionByName(
            new_inserted_versions,
            allowMissingColumns=True,
        )
        .unionByName(
            new_updated_versions,
            allowMissingColumns=True,
        )
        .cache()
    )

    validate_single_current_record(
        dataframe=final_dimension,
        primary_keys=primary_keys,
        dataset_name=dataset_name,
    )

    validate_date_ranges(
        dataframe=final_dimension,
        dataset_name=dataset_name,
    )

    validate_current_end_dates(
        dataframe=final_dimension,
        dataset_name=dataset_name,
    )

    final_count = final_dimension.count()

    expected_count = (
        previous_count
        + inserted_count
        + updated_count
    )

    if final_count != expected_count:
        raise ValueError(
            f"SCD2 reconciliation failed for "
            f"{dataset_name}: "
            f"expected={expected_count}, "
            f"actual={final_count}"
        )

    current_count = (
        final_dimension
        .filter(F.col("is_current"))
        .count()
    )

    expired_count = (
        final_dimension
        .filter(~F.col("is_current"))
        .count()
    )

    output_path = write_dimension(
        dataframe=final_dimension,
        output_root=SCD2_ROOT,
        batch_date=batch_date,
        dataset_name=dataset_name,
    )

    print(
        f"{dataset_name:<12} | "
        f"Previous: {previous_count:>8,} | "
        f"Inserted: {inserted_count:>6,} | "
        f"Updated: {updated_count:>6,} | "
        f"Deleted: {deleted_count:>6,} | "
        f"History: {final_count:>8,} | "
        f"Current: {current_count:>8,}"
    )

    audit_record = {
        "dataset_name": dataset_name,
        "batch_date": batch_date,
        "previous_batch_date": previous_batch_date,
        "operation": "INCREMENTAL_MERGE",
        "previous_record_count": previous_count,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "deleted_count": deleted_count,
        "expected_record_count": expected_count,
        "final_record_count": final_count,
        "current_record_count": current_count,
        "expired_record_count": expired_count,
        "counts_match": final_count == expected_count,
        "output_path": str(output_path),
        "status": "SUCCESS",
    }

    previous_dimension.unpersist()
    inserted_cdc.unpersist()
    updated_cdc.unpersist()
    deleted_cdc.unpersist()
    final_dimension.unpersist()

    return audit_record


def write_audit(
    spark,
    audit_records: list[dict[str, Any]],
    batch_date: str,
) -> str:
    """
    Write the SCD2 audit using Spark.
    Compatible with Local + ADLS.
    """

    output_path = join_storage_path(
        SCD2_AUDIT_ROOT,
        f"batch_date={batch_date}",
    )

    payload = {
        "batch_date": batch_date,
        "audit_timestamp": datetime.now().isoformat(),
        "datasets": audit_records,
    }

    audit_df = spark.createDataFrame(
        [(str(payload),)],
        ["json_payload"],
    )

    (
        audit_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .text(output_path)
    )

    return output_path


def run_scd2_pipeline(
    batch_date: str,
    initial_batch_date: str | None = None,
) -> None:
    """
    Initialise or incrementally update SCD2 dimensions.
    """

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        if initial_batch_date:
            initialise_scd2_batch(
                spark=spark,
                initial_batch_date=initial_batch_date,
            )

            print("=" * 110)
            print(
                "SCD TYPE 2 INITIALISATION "
                "COMPLETED SUCCESSFULLY"
            )
            print("=" * 110)
            return

        previous_batch_date = find_previous_scd2_batch(
            spark=spark,
            scd2_root=SCD2_ROOT,
            current_batch_date=batch_date,
        )

        if previous_batch_date is None:
            raise ValueError(
                "No previous SCD2 batch exists. "
                "Run the initialisation command first."
            )

        cdc_path = join_storage_path(
            CDC_ROOT,
            f"batch_date={batch_date}",
        )

        if not spark_path_exists(
            spark=spark,
            path=cdc_path,
        ):
            raise FileNotFoundError(
                f"CDC batch not found: {cdc_path}"
            )

        print("=" * 135)
        print(
            f"SCD TYPE 2 MERGE — "
            f"CURRENT BATCH: {batch_date}"
        )
        print(
            f"PREVIOUS SCD2 BATCH: "
            f"{previous_batch_date}"
        )
        print("=" * 135)

        audit_records = []

        for dataset_name in DIMENSION_DATASETS:
            record = process_dimension(
                spark=spark,
                dataset_name=dataset_name,
                batch_date=batch_date,
                previous_batch_date=previous_batch_date,
            )

            audit_records.append(record)

        audit_path = write_audit(
            spark=spark,
            audit_records=audit_records,
            batch_date=batch_date,
        )

        print("=" * 135)
        print(
            "SCD TYPE 2 MERGE COMPLETED SUCCESSFULLY"
        )
        dimension_output = join_storage_path(
            SCD2_ROOT,
            f"batch_date={batch_date}",
        )

        print(
            f"Dimension output: {dimension_output}"
        )
        print(f"Audit report: {audit_path}")
        print("=" * 135)

    finally:
        spark.stop()