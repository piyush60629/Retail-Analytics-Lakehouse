from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.ingestion.schemas import (
    DATASET_PRIMARY_KEYS,
)
from src.incremental_silver.config import (
    CDC_METADATA_COLUMNS,
    CDC_ROOT,
    DATASETS,
    INCREMENTAL_SILVER_AUDIT_ROOT,
    SILVER_ROOT,
)
from src.incremental_silver.merge_utils import (
    build_incremental_snapshot,
    drop_columns_if_present,
    find_previous_silver_batch,
    read_cdc_change,
    read_previous_silver,
    validate_unique_primary_keys,
    write_silver_snapshot,
)
from src.silver.spark_session import (
    create_spark_session,
)
from src.silver.transformers import (
    TRANSFORMERS,
)


def combine_inserted_and_updated(
    inserted_dataframe: DataFrame,
    updated_dataframe: DataFrame,
) -> DataFrame:
    """
    Combine CDC inserted and updated records.
    """

    return inserted_dataframe.unionByName(
        updated_dataframe,
        allowMissingColumns=True,
    )


def create_audit_record(
    batch_date: str,
    previous_batch_date: str,
    dataset_name: str,
    previous_count: int,
    inserted_count: int,
    updated_count: int,
    deleted_count: int,
    final_count: int,
    output_path: str,
) -> dict[str, Any]:
    """
    Create one incremental Silver audit record.
    """

    expected_count = (
        previous_count
        + inserted_count
        - deleted_count
    )

    return {
        "batch_date": batch_date,
        "previous_batch_date": previous_batch_date,
        "dataset_name": dataset_name,
        "previous_record_count": previous_count,
        "inserted_record_count": inserted_count,
        "updated_record_count": updated_count,
        "deleted_record_count": deleted_count,
        "expected_record_count": expected_count,
        "final_record_count": final_count,
        "counts_match": expected_count == final_count,
        "status": (
            "SUCCESS"
            if expected_count == final_count
            else "FAILED"
        ),
        "output_path": output_path,
    }


def process_dataset(
    spark,
    dataset_name: str,
    batch_date: str,
    previous_batch_date: str,
) -> dict[str, Any]:
    """
    Incrementally merge one dataset into Silver.
    """

    primary_keys = DATASET_PRIMARY_KEYS[
        dataset_name
    ]

    previous_silver = read_previous_silver(
        spark=spark,
        silver_root=SILVER_ROOT,
        batch_date=previous_batch_date,
        dataset_name=dataset_name,
    ).cache()

    inserted_cdc = read_cdc_change(
        spark=spark,
        cdc_root=CDC_ROOT,
        batch_date=batch_date,
        dataset_name=dataset_name,
        change_type="inserted",
    ).cache()

    updated_cdc = read_cdc_change(
        spark=spark,
        cdc_root=CDC_ROOT,
        batch_date=batch_date,
        dataset_name=dataset_name,
        change_type="updated",
    ).cache()

    deleted_cdc = read_cdc_change(
        spark=spark,
        cdc_root=CDC_ROOT,
        batch_date=batch_date,
        dataset_name=dataset_name,
        change_type="deleted",
    ).cache()

    previous_count = previous_silver.count()
    inserted_count = inserted_cdc.count()
    updated_count = updated_cdc.count()
    deleted_count = deleted_cdc.count()

    upsert_dataframe = combine_inserted_and_updated(
        inserted_dataframe=inserted_cdc,
        updated_dataframe=updated_cdc,
    )

    upsert_dataframe = drop_columns_if_present(
        dataframe=upsert_dataframe,
        column_names=CDC_METADATA_COLUMNS,
    )

    transformer = TRANSFORMERS[dataset_name]

    transformed_upserts = transformer(
        upsert_dataframe,
        batch_date,
    )

    merged_dataframe = build_incremental_snapshot(
        previous_silver=previous_silver,
        transformed_upserts=transformed_upserts,
        updated_cdc=updated_cdc,
        deleted_cdc=deleted_cdc,
        primary_keys=primary_keys,
    ).cache()

    validate_unique_primary_keys(
        dataframe=merged_dataframe,
        primary_keys=primary_keys,
        dataset_name=dataset_name,
    )

    final_count = merged_dataframe.count()

    expected_count = (
        previous_count
        + inserted_count
        - deleted_count
    )

    if expected_count != final_count:
        raise ValueError(
            f"Silver reconciliation failed for "
            f"{dataset_name}: "
            f"expected={expected_count}, "
            f"actual={final_count}"
        )

    output_path = write_silver_snapshot(
        dataframe=merged_dataframe,
        silver_root=SILVER_ROOT,
        batch_date=batch_date,
        dataset_name=dataset_name,
    )

    print(
        f"{dataset_name:<12} | "
        f"Previous: {previous_count:>8,} | "
        f"Inserted: {inserted_count:>6,} | "
        f"Updated: {updated_count:>6,} | "
        f"Deleted: {deleted_count:>6,} | "
        f"Final: {final_count:>8,}"
    )

    audit_record = create_audit_record(
        batch_date=batch_date,
        previous_batch_date=previous_batch_date,
        dataset_name=dataset_name,
        previous_count=previous_count,
        inserted_count=inserted_count,
        updated_count=updated_count,
        deleted_count=deleted_count,
        final_count=final_count,
        output_path=str(output_path),
    )

    previous_silver.unpersist()
    inserted_cdc.unpersist()
    updated_cdc.unpersist()
    deleted_cdc.unpersist()
    merged_dataframe.unpersist()

    return audit_record


def write_incremental_audit(
    spark,
    audit_records: list[dict[str, Any]],
    batch_date: str,
) -> str:
    """
    Write the incremental Silver audit as CSV.
    """

    output_path = (
        INCREMENTAL_SILVER_AUDIT_ROOT
        / f"batch_date={batch_date}"
    )

    audit_dataframe = spark.createDataFrame(
        audit_records
    )

    (
        audit_dataframe
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(output_path))
    )

    return str(output_path)


def run_incremental_silver(
    batch_date: str,
) -> None:
    """
    Build a new Silver snapshot using CDC changes.
    """

    previous_batch_date = find_previous_silver_batch(
        silver_root=SILVER_ROOT,
        current_batch_date=batch_date,
    )

    if previous_batch_date is None:
        raise ValueError(
            "No previous Silver batch was found. "
            "Run the normal Silver pipeline for the initial "
            "batch before using incremental Silver."
        )

    cdc_batch_path = (
        CDC_ROOT
        / f"batch_date={batch_date}"
    )

    if not cdc_batch_path.exists():
        raise FileNotFoundError(
            f"CDC batch not found: {cdc_batch_path}"
        )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    audit_records = []

    try:
        print("=" * 125)
        print(
            f"INCREMENTAL SILVER MERGE — "
            f"CURRENT BATCH: {batch_date}"
        )
        print(
            f"PREVIOUS SILVER BATCH: "
            f"{previous_batch_date}"
        )
        print("=" * 125)

        for dataset_name in DATASETS:
            audit_record = process_dataset(
                spark=spark,
                dataset_name=dataset_name,
                batch_date=batch_date,
                previous_batch_date=previous_batch_date,
            )

            audit_records.append(audit_record)

        audit_output = write_incremental_audit(
            spark=spark,
            audit_records=audit_records,
            batch_date=batch_date,
        )

        failed_records = [
            record
            for record in audit_records
            if record["status"] != "SUCCESS"
        ]

        if failed_records:
            raise RuntimeError(
                "One or more incremental Silver "
                "reconciliation checks failed."
            )

        print("=" * 125)
        print(
            "INCREMENTAL SILVER MERGE "
            "COMPLETED SUCCESSFULLY"
        )
        print(
            f"Silver batch: "
            f"{SILVER_ROOT / f'batch_date={batch_date}'}"
        )
        print(f"Audit output: {audit_output}")
        print("=" * 125)

    finally:
        spark.stop()