from __future__ import annotations
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.ingestion.schemas import (
    DATASET_PRIMARY_KEYS,
    DATASET_SCHEMAS,
)




BASE_DIR = Path(__file__).resolve().parents[2]

PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
BRONZE_DATA_DIR = BASE_DIR / "data" / "bronze"


DATASETS = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]


def create_spark_session() -> SparkSession:
    """Create and configure the local Spark session."""

    spark = (
        SparkSession.builder
        .appName("RetailAnalyticsBronzeIngestion")
        .master("local[*]")
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


def load_processed_csv(
    spark: SparkSession,
    input_path: Path,
    schema: StructType,
) -> DataFrame:
    """Read a validated CSV using an explicit Spark schema."""

    if not input_path.exists():
        raise FileNotFoundError(
            f"Processed input file not found: {input_path}"
        )

    dataframe = (
        spark.read
        .option("header", True)
        .option("mode", "FAILFAST")
        .option(
            "timestampFormat",
            "yyyy-MM-dd HH:mm:ss.SSSSSS",
        )
        .schema(schema)
        .csv(str(input_path))
    )

    return dataframe


def build_record_hash(
    dataframe: DataFrame,
    excluded_columns: set[str] | None = None,
) -> DataFrame:
    """
    Create a deterministic SHA-256 hash using business columns.

    Technical metadata columns are excluded from the hash.
    """

    exclusions = excluded_columns or set()

    hash_columns = [
        F.coalesce(
            F.col(column).cast("string"),
            F.lit("NULL"),
        )
        for column in dataframe.columns
        if column not in exclusions
    ]

    return dataframe.withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *hash_columns,
            ),
            256,
        ),
    )


def add_technical_metadata(
    dataframe: DataFrame,
    batch_date: str,
    source_file: str,
) -> DataFrame:
    """Add standard Bronze technical metadata columns."""

    dataframe = (
        dataframe
        .withColumn(
            "batch_date",
            F.to_date(F.lit(batch_date)),
        )
        .withColumn(
            "source_file",
            F.lit(source_file),
        )
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp(),
        )
    )

    dataframe = build_record_hash(
        dataframe,
        excluded_columns={
            "validation_timestamp",
            "batch_date",
            "source_file",
            "ingestion_timestamp",
            "record_hash",
        },
    )

    return dataframe


def validate_primary_keys(
    dataframe: DataFrame,
    primary_keys: list[str],
    dataset_name: str,
) -> None:
    """Ensure no null or duplicate primary keys reach Bronze."""

    null_condition = None

    for primary_key in primary_keys:
        current_condition = F.col(primary_key).isNull()

        null_condition = (
            current_condition
            if null_condition is None
            else null_condition | current_condition
        )

    null_primary_key_count = (
        dataframe
        .filter(null_condition)
        .count()
    )

    duplicate_primary_key_count = (
        dataframe
        .groupBy(*primary_keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if null_primary_key_count > 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{null_primary_key_count} null primary keys."
        )

    if duplicate_primary_key_count > 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{duplicate_primary_key_count} duplicate primary keys."
        )


def save_bronze_table(
    dataframe: DataFrame,
    output_path: Path,
) -> None:
    """Write a Bronze dataset in Parquet format."""

    (
        dataframe.write
        .mode("overwrite")
        .partitionBy("batch_date")
        .parquet(str(output_path))
    )


def build_audit_record(
    dataset_name: str,
    source_file: str,
    source_count: int,
    bronze_count: int,
    output_path: Path,
) -> dict[str, Any]:
    """Create one Bronze ingestion audit record."""

    counts_match = source_count == bronze_count

    return {
        "dataset_name": dataset_name,
        "source_file": source_file,
        "source_record_count": source_count,
        "bronze_record_count": bronze_count,
        "record_counts_match": counts_match,
        "bronze_output_path": str(output_path),
        "ingestion_timestamp": datetime.now().isoformat(),
        "status": "SUCCESS" if counts_match else "FAILED",
    }


def ingest_dataset(
    spark: SparkSession,
    dataset_name: str,
    batch_date: str,
    processed_batch_directory: Path,
    bronze_batch_directory: Path,
) -> dict[str, Any]:
    """Ingest one processed dataset into the Bronze layer."""

    source_file = f"{dataset_name}_{batch_date}.csv"

    input_path = (
        processed_batch_directory
        / source_file
    )

    output_path = (
        bronze_batch_directory
        / dataset_name
    )

    schema = DATASET_SCHEMAS[dataset_name]
    primary_keys = DATASET_PRIMARY_KEYS[dataset_name]

    dataframe = load_processed_csv(
        spark=spark,
        input_path=input_path,
        schema=schema,
    )

    source_count = dataframe.count()

    validate_primary_keys(
        dataframe=dataframe,
        primary_keys=primary_keys,
        dataset_name=dataset_name,
    )

    bronze_dataframe = add_technical_metadata(
        dataframe=dataframe,
        batch_date=batch_date,
        source_file=source_file,
    )

    save_bronze_table(
        dataframe=bronze_dataframe,
        output_path=output_path,
    )

    bronze_count = (
        spark.read
        .parquet(str(output_path))
        .count()
    )

    audit_record = build_audit_record(
        dataset_name=dataset_name,
        source_file=source_file,
        source_count=source_count,
        bronze_count=bronze_count,
        output_path=output_path,
    )

    if source_count != bronze_count:
        raise RuntimeError(
            f"Row-count reconciliation failed for "
            f"{dataset_name}. Source={source_count}, "
            f"Bronze={bronze_count}"
        )

    print(
        f"{dataset_name:<12} "
        f"Source: {source_count:>8,} | "
        f"Bronze: {bronze_count:>8,} | "
        f"Status: SUCCESS"
    )

    return audit_record


def run_bronze_ingestion(batch_date: str) -> None:
    """Run Bronze ingestion for all validated datasets."""

    processed_batch_directory = (
        PROCESSED_DATA_DIR
        / f"batch_date={batch_date}"
    )

    bronze_batch_directory = (
        BRONZE_DATA_DIR
        / f"batch_date={batch_date}"
    )

    if not processed_batch_directory.exists():
        raise FileNotFoundError(
            "Processed batch directory was not found: "
            f"{processed_batch_directory}"
        )

    if bronze_batch_directory.exists():
        shutil.rmtree(bronze_batch_directory)

    bronze_batch_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    spark = create_spark_session()

    audit_records: list[dict[str, Any]] = []

    print("=" * 90)
    print(f"BRONZE INGESTION — BATCH DATE: {batch_date}")
    print("=" * 90)

    try:
        for dataset_name in DATASETS:
            audit_record = ingest_dataset(
                spark=spark,
                dataset_name=dataset_name,
                batch_date=batch_date,
                processed_batch_directory=(
                    processed_batch_directory
                ),
                bronze_batch_directory=(
                    bronze_batch_directory
                ),
            )

            audit_records.append(audit_record)

        audit_output_path = (
            bronze_batch_directory
            / f"bronze_audit_{batch_date}.json"
        )

        with audit_output_path.open(
            "w",
            encoding="utf-8",
        ) as audit_file:
            json.dump(
                audit_records,
                audit_file,
                indent=4,
            )

        total_source_records = sum(
            record["source_record_count"]
            for record in audit_records
        )

        total_bronze_records = sum(
            record["bronze_record_count"]
            for record in audit_records
        )

        print("\n" + "-" * 90)
        print(
            f"Total source records: {total_source_records:,}"
        )
        print(
            f"Total Bronze records: {total_bronze_records:,}"
        )
        print(
            "Overall reconciliation: "
            f"{'SUCCESS' if total_source_records == total_bronze_records else 'FAILED'}"
        )
        print("-" * 90)

        print(f"\nBronze location: {bronze_batch_directory}")
        print(f"Audit report: {audit_output_path}")

        print("\n" + "=" * 90)
        print("BRONZE INGESTION COMPLETED SUCCESSFULLY")
        print("=" * 90)

    finally:
        spark.stop()


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description=(
            "Ingest validated retail data into "
            "the Bronze Parquet layer."
        )
    )

    parser.add_argument(
        "--batch-date",
        required=True,
        help="Batch date in YYYY-MM-DD format.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    run_bronze_ingestion(
        batch_date=arguments.batch_date
    )


if __name__ == "__main__":
    main()