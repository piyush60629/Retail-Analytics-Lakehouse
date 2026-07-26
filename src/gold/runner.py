from __future__ import annotations

from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_storage import (
    spark_path_exists,
)
from src.common.storage_config import (
    join_storage_path,
)
from src.gold.config import (
    ENRICHED_FACT_ROOT,
    GOLD_AUDIT_ROOT,
    GOLD_ROOT,
    SCD2_ROOT,
)
from src.gold.dimensions import (
    build_dim_customer,
    build_dim_date,
    build_dim_product,
)
from src.gold.facts import (
    build_fact_payment,
    build_fact_sales,
)
from src.silver.spark_session import (
    create_spark_session,
)


def read_scd2_dimension(
    spark: SparkSession,
    dataset_name: str,
    batch_date: str,
) -> DataFrame:
    """
    Read an SCD Type 2 dimension snapshot
    from local storage or ADLS Gen2.
    """

    input_path = join_storage_path(
        SCD2_ROOT,
        f"batch_date={batch_date}",
        dataset_name,
    )

    if not spark_path_exists(
        spark=spark,
        path=input_path,
    ):
        raise FileNotFoundError(
            f"SCD2 dimension not found: {input_path}"
        )

    return spark.read.parquet(
        input_path
    )


def read_enriched_fact(
    spark: SparkSession,
    dataset_name: str,
    batch_date: str,
) -> DataFrame:
    """
    Read a fact dataset containing surrogate keys
    from local storage or ADLS Gen2.
    """

    input_path = join_storage_path(
        ENRICHED_FACT_ROOT,
        f"batch_date={batch_date}",
        dataset_name,
    )

    if not spark_path_exists(
        spark=spark,
        path=input_path,
    ):
        raise FileNotFoundError(
            f"Enriched fact not found: {input_path}"
        )

    return spark.read.parquet(
        input_path
    )


def write_gold_table(
    dataframe: DataFrame,
    table_name: str,
    batch_date: str,
) -> str:
    """
    Write one Gold table to local storage
    or ADLS Gen2.
    """

    output_path = join_storage_path(
        GOLD_ROOT,
        f"batch_date={batch_date}",
        table_name,
    )

    (
        dataframe
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    return output_path


def write_gold_audit(
    spark: SparkSession,
    audit_records: List[Dict[str, object]],
    batch_date: str,
) -> str:
    """
    Write Gold pipeline audit records.
    """

    output_path = join_storage_path(
        GOLD_AUDIT_ROOT,
        f"batch_date={batch_date}",
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
        .csv(output_path)
    )

    return output_path


def process_gold_table(
    dataframe: DataFrame,
    table_name: str,
    batch_date: str,
    audit_records: List[Dict[str, object]],
    key_columns: List[str],
    required_columns: List[str] | None = None,
) -> None:
    """
    Validate and write one Gold table.
    """

    dataframe = dataframe.cache()

    try:
        row_count = dataframe.count()

        if row_count == 0:
            raise ValueError(
                f"Gold table {table_name} has no records."
            )

        missing_key_columns = [
            column_name
            for column_name in key_columns
            if column_name not in dataframe.columns
        ]

        if missing_key_columns:
            raise ValueError(
                f"Gold table {table_name} is missing "
                f"key columns: {missing_key_columns}"
            )

        duplicate_count = (
            dataframe
            .groupBy(*key_columns)
            .count()
            .filter("count > 1")
            .count()
        )

        if duplicate_count > 0:
            raise ValueError(
                f"Gold table {table_name} contains "
                f"{duplicate_count} duplicate keys."
            )

        for column_name in required_columns or []:
            if column_name not in dataframe.columns:
                raise ValueError(
                    f"Gold table {table_name} is missing "
                    f"required column: {column_name}"
                )

            null_count = (
                dataframe
                .filter(
                    dataframe[column_name].isNull()
                )
                .count()
            )

            if null_count > 0:
                raise ValueError(
                    f"Gold table {table_name} contains "
                    f"{null_count} null values in "
                    f"{column_name}."
                )

        output_path = write_gold_table(
            dataframe=dataframe,
            table_name=table_name,
            batch_date=batch_date,
        )

        audit_records.append(
            {
                "batch_date": batch_date,
                "table_name": table_name,
                "record_count": row_count,
                "status": "SUCCESS",
                "output_path": output_path,
            }
        )

        print(
            f"{table_name:<20} "
            f"Records: {row_count:>8} | "
            f"Status: SUCCESS"
        )

    finally:
        dataframe.unpersist()


def run_gold_pipeline(
    batch_date: str,
) -> None:
    """
    Build the complete Gold dimensional model.
    """

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    audit_records: List[Dict[str, object]] = []

    try:
        print("=" * 115)
        print(
            f"GOLD DIMENSIONAL MODEL — "
            f"BATCH DATE: {batch_date}"
        )
        print("=" * 115)

        customers = read_scd2_dimension(
            spark=spark,
            dataset_name="customers",
            batch_date=batch_date,
        )

        products = read_scd2_dimension(
            spark=spark,
            dataset_name="products",
            batch_date=batch_date,
        )

        orders = read_enriched_fact(
            spark=spark,
            dataset_name="orders",
            batch_date=batch_date,
        )

        order_items = read_enriched_fact(
            spark=spark,
            dataset_name="order_items",
            batch_date=batch_date,
        )

        payments = read_enriched_fact(
            spark=spark,
            dataset_name="payments",
            batch_date=batch_date,
        )

        dim_customer = build_dim_customer(
            customers,
            batch_date,
        )

        dim_product = build_dim_product(
            products,
            batch_date,
        )

        dim_date = build_dim_date(
            spark=spark,
            orders=orders,
            payments=payments,
            batch_date=batch_date,
        )

        fact_sales = build_fact_sales(
            orders=orders,
            order_items=order_items,
            dim_customer=dim_customer,
            dim_product=dim_product,
            batch_date=batch_date,
        )

        fact_payment = build_fact_payment(
            payments=payments,
            orders=orders,
            dim_customer=dim_customer,
            batch_date=batch_date,
        )

        gold_tables = {
            "dim_customer": {
                "dataframe": dim_customer,
                "key_columns": [
                    "customer_sk"
                ],
                "required_columns": [
                    "customer_sk"
                ],
            },
            "dim_product": {
                "dataframe": dim_product,
                "key_columns": [
                    "product_sk"
                ],
                "required_columns": [
                    "product_sk"
                ],
            },
            "dim_date": {
                "dataframe": dim_date,
                "key_columns": [
                    "date_key"
                ],
                "required_columns": [
                    "date_key"
                ],
            },
            "fact_sales": {
                "dataframe": fact_sales,
                "key_columns": [
                    "sales_sk"
                ],
                "required_columns": [
                    "sales_sk",
                    "order_sk",
                    "customer_sk",
                    "product_sk",
                ],
            },
            "fact_payment": {
                "dataframe": fact_payment,
                "key_columns": [
                    "payment_sk"
                ],
                "required_columns": [
                    "payment_sk",
                    "order_sk",
                    "customer_sk",
                ],
            },
        }

        for (
            table_name,
            table_config,
        ) in gold_tables.items():
            process_gold_table(
                dataframe=table_config[
                    "dataframe"
                ],
                table_name=table_name,
                batch_date=batch_date,
                audit_records=audit_records,
                key_columns=table_config[
                    "key_columns"
                ],
                required_columns=table_config[
                    "required_columns"
                ],
            )

        audit_output_path = write_gold_audit(
            spark=spark,
            audit_records=audit_records,
            batch_date=batch_date,
        )

        print("=" * 115)
        print("GOLD DIMENSIONAL MODEL COMPLETED")
        print(f"Gold output:  {GOLD_ROOT}")
        print(f"Audit output: {audit_output_path}")
        print("=" * 115)

    finally:
        spark.stop()