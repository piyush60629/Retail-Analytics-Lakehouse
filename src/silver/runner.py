from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from src.silver.config import (
    BRONZE_ROOT,
    DATASETS,
    SILVER_AUDIT_ROOT,
    SILVER_ROOT,
)
from src.silver.spark_session import (
    create_spark_session,
)
from src.silver.transformers import TRANSFORMERS


def read_bronze_dataset(
    spark: SparkSession,
    dataset_name: str,
    batch_date: str,
) -> DataFrame:
    """
    Read one dataset from the Bronze Parquet layer.
    """

    input_path = (
        BRONZE_ROOT
        / f"batch_date={batch_date}"
        / dataset_name
    )

    if not input_path.exists():
        raise FileNotFoundError(
            f"Bronze dataset not found: {input_path}"
        )

    return spark.read.parquet(str(input_path))


def write_silver_dataset(
    dataframe: DataFrame,
    dataset_name: str,
    batch_date: str,
) -> str:
    output_path = (
        SILVER_ROOT
        / f"batch_date={batch_date}"
        / dataset_name
    )

    (
        dataframe
        .write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    return str(output_path)


def write_silver_audit(
    spark: SparkSession,
    audit_records: List[Dict[str, object]],
    batch_date: str,
) -> str:
    audit_output_path = (
        SILVER_AUDIT_ROOT
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
        .csv(str(audit_output_path))
    )

    return str(audit_output_path)


def run_silver_pipeline(
    batch_date: str,
) -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    audit_records: List[Dict[str, object]] = []

    try:
        print("=" * 115)
        print(
            f"SILVER TRANSFORMATION — "
            f"BATCH DATE: {batch_date}"
        )
        print("=" * 115)

        for dataset_name in DATASETS:
            bronze_dataframe = read_bronze_dataset(
                spark=spark,
                dataset_name=dataset_name,
                batch_date=batch_date,
            )

            input_count = bronze_dataframe.count()

            transformer = TRANSFORMERS[dataset_name]

            silver_dataframe = transformer(
                bronze_dataframe,
                batch_date,
            ).cache()

            output_count = silver_dataframe.count()

            if input_count != output_count:
                raise ValueError(
                    f"Row-count mismatch for "
                    f"{dataset_name}: "
                    f"input={input_count}, "
                    f"output={output_count}"
                )

            output_path = write_silver_dataset(
                dataframe=silver_dataframe,
                dataset_name=dataset_name,
                batch_date=batch_date,
            )

            audit_records.append(
                {
                    "batch_date": batch_date,
                    "dataset_name": dataset_name,
                    "input_records": input_count,
                    "output_records": output_count,
                    "rejected_records": (
                        input_count - output_count
                    ),
                    "status": "SUCCESS",
                    "output_path": output_path,
                }
            )

            print(
                f"{dataset_name:<13} "
                f"Input: {input_count:>8} | "
                f"Output: {output_count:>8} | "
                f"Status: SUCCESS"
            )

            silver_dataframe.unpersist()

        audit_output_path = write_silver_audit(
            spark=spark,
            audit_records=audit_records,
            batch_date=batch_date,
        )

        print("=" * 115)
        print("SILVER TRANSFORMATION COMPLETED")
        print(f"Silver output: {SILVER_ROOT}")
        print(f"Audit output:  {audit_output_path}")
        print("=" * 115)

    finally:
        spark.stop()