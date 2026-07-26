from __future__ import annotations

from datetime import datetime

from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_storage import (
    spark_path_exists,
)
from src.common.storage_config import (
    join_storage_path,
)

from src.gold_quality.checks import (
    CheckResult,
    check_current_scd_end_date,
    check_current_scd_uniqueness,
    check_date_key_consistency,
    check_foreign_key,
    check_non_negative_columns,
    check_not_null,
    check_payment_amount_equation,
    check_positive_column,
    check_row_count_reconciliation,
    check_sales_amount_equation,
    check_scd_date_range,
    check_unique_key,
)
from src.gold_quality.config import (
    ENRICHED_FACT_ROOT,
    GOLD_ROOT,
    QUALITY_AUDIT_ROOT,
)
from src.silver.spark_session import (
    create_spark_session,
)


def read_parquet_dataset(
    spark: SparkSession,
    root_path: str,
    dataset_name: str,
    batch_date: str,
) -> DataFrame:
    """
    Read a batch-partitioned Parquet dataset
    from local storage or ADLS Gen2.
    """

    input_path = join_storage_path(
        root_path,
        f"batch_date={batch_date}",
        dataset_name,
    )

    if not spark_path_exists(
        spark=spark,
        path=input_path,
    ):
        raise FileNotFoundError(
            f"Dataset not found: {input_path}"
        )

    return spark.read.parquet(
        input_path
    )


def write_quality_audit(
    spark: SparkSession,
    results: List[CheckResult],
    batch_date: str,
) -> str:
    """
    Write quality-check results as CSV
    to local storage or ADLS Gen2.
    """

    output_path = join_storage_path(
        QUALITY_AUDIT_ROOT,
        f"batch_date={batch_date}",
    )

    audit_records: List[Dict[str, object]] = []

    execution_timestamp = (
        datetime.now().isoformat(
            timespec="seconds"
        )
    )

    for result in results:
        audit_records.append(
            {
                "batch_date": batch_date,
                "execution_timestamp": (
                    execution_timestamp
                ),
                "check_name": result[
                    "check_name"
                ],
                "table_name": result[
                    "table_name"
                ],
                "status": result["status"],
                "failed_count": int(
                    result["failed_count"]
                ),
                "description": result[
                    "description"
                ],
            }
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


def print_check_result(
    result: CheckResult,
) -> None:
    """
    Print one check result in a readable format.
    """

    status = str(result["status"])
    table_name = str(result["table_name"])
    check_name = str(result["check_name"])
    failed_count = int(
        result["failed_count"]
    )

    print(
        f"{table_name:<20} "
        f"{check_name:<38} "
        f"{status:<5} "
        f"Failed: {failed_count}"
    )


def run_gold_quality_pipeline(
    batch_date: str,
) -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    results: List[CheckResult] = []

    cached_dataframes: List[DataFrame] = []

    try:
        print("=" * 125)
        print(
            "GOLD DATA QUALITY CHECKS — "
            f"BATCH DATE: {batch_date}"
        )
        print("=" * 125)

        dim_customer = read_parquet_dataset(
            spark=spark,
            root_path=GOLD_ROOT,
            dataset_name="dim_customer",
            batch_date=batch_date,
        ).cache()

        dim_product = read_parquet_dataset(
            spark=spark,
            root_path=GOLD_ROOT,
            dataset_name="dim_product",
            batch_date=batch_date,
        ).cache()

        dim_date = read_parquet_dataset(
            spark=spark,
            root_path=GOLD_ROOT,
            dataset_name="dim_date",
            batch_date=batch_date,
        ).cache()

        fact_sales = read_parquet_dataset(
            spark=spark,
            root_path=GOLD_ROOT,
            dataset_name="fact_sales",
            batch_date=batch_date,
        ).cache()

        fact_payment = read_parquet_dataset(
            spark=spark,
            root_path=GOLD_ROOT,
            dataset_name="fact_payment",
            batch_date=batch_date,
        ).cache()

        enriched_order_items = (
            read_parquet_dataset(
                spark=spark,
                root_path=ENRICHED_FACT_ROOT,
                dataset_name="order_items",
                batch_date=batch_date,
            )
            .cache()
        )

        enriched_payments = (
            read_parquet_dataset(
                spark=spark,
                root_path=ENRICHED_FACT_ROOT,
                dataset_name="payments",
                batch_date=batch_date,
            )
            .cache()
        )

        cached_dataframes.extend(
            [
                dim_customer,
                dim_product,
                dim_date,
                fact_sales,
                fact_payment,
                enriched_order_items,
                enriched_payments,
            ]
        )

        # Dimension primary-key checks
        results.extend(
            check_not_null(
                dataframe=dim_customer,
                table_name="dim_customer",
                columns=[
                    "customer_sk",
                    "customer_id",
                    "effective_from",
                    "record_version",
                    "is_current",
                ],
            )
        )

        results.append(
            check_unique_key(
                dataframe=dim_customer,
                table_name="dim_customer",
                key_columns=["customer_sk"],
            )
        )

        results.extend(
            check_not_null(
                dataframe=dim_product,
                table_name="dim_product",
                columns=[
                    "product_sk",
                    "product_id",
                    "effective_from",
                    "record_version",
                    "is_current",
                ],
            )
        )

        results.append(
            check_unique_key(
                dataframe=dim_product,
                table_name="dim_product",
                key_columns=["product_sk"],
            )
        )

        results.extend(
            check_not_null(
                dataframe=dim_date,
                table_name="dim_date",
                columns=[
                    "date_key",
                    "calendar_date",
                ],
            )
        )

        results.append(
            check_unique_key(
                dataframe=dim_date,
                table_name="dim_date",
                key_columns=["date_key"],
            )
        )

        # Fact primary-key checks
        results.extend(
            check_not_null(
                dataframe=fact_sales,
                table_name="fact_sales",
                columns=[
                    "sales_sk",
                    "order_sk",
                    "customer_sk",
                    "product_sk",
                    "order_date_key",
                ],
            )
        )

        results.append(
            check_unique_key(
                dataframe=fact_sales,
                table_name="fact_sales",
                key_columns=["sales_sk"],
            )
        )

        results.extend(
            check_not_null(
                dataframe=fact_payment,
                table_name="fact_payment",
                columns=[
                    "payment_sk",
                    "order_sk",
                    "customer_sk",
                    "payment_date_key",
                ],
            )
        )

        results.append(
            check_unique_key(
                dataframe=fact_payment,
                table_name="fact_payment",
                key_columns=["payment_sk"],
            )
        )

        # Foreign-key checks
        results.append(
            check_foreign_key(
                fact_dataframe=fact_sales,
                dimension_dataframe=dim_customer,
                fact_table_name="fact_sales",
                dimension_table_name=(
                    "dim_customer"
                ),
                fact_key="customer_sk",
                dimension_key="customer_sk",
            )
        )

        results.append(
            check_foreign_key(
                fact_dataframe=fact_sales,
                dimension_dataframe=dim_product,
                fact_table_name="fact_sales",
                dimension_table_name=(
                    "dim_product"
                ),
                fact_key="product_sk",
                dimension_key="product_sk",
            )
        )

        results.append(
            check_foreign_key(
                fact_dataframe=fact_sales,
                dimension_dataframe=dim_date,
                fact_table_name="fact_sales",
                dimension_table_name="dim_date",
                fact_key="order_date_key",
                dimension_key="date_key",
            )
        )

        results.append(
            check_foreign_key(
                fact_dataframe=fact_payment,
                dimension_dataframe=dim_customer,
                fact_table_name="fact_payment",
                dimension_table_name=(
                    "dim_customer"
                ),
                fact_key="customer_sk",
                dimension_key="customer_sk",
            )
        )

        results.append(
            check_foreign_key(
                fact_dataframe=fact_payment,
                dimension_dataframe=dim_date,
                fact_table_name="fact_payment",
                dimension_table_name="dim_date",
                fact_key="payment_date_key",
                dimension_key="date_key",
            )
        )

        # Source-to-Gold row reconciliation
        results.append(
            check_row_count_reconciliation(
                source_dataframe=(
                    enriched_order_items
                ),
                target_dataframe=fact_sales,
                source_name=(
                    "enriched_order_items"
                ),
                target_name="fact_sales",
            )
        )

        results.append(
            check_row_count_reconciliation(
                source_dataframe=(
                    enriched_payments
                ),
                target_dataframe=fact_payment,
                source_name=(
                    "enriched_payments"
                ),
                target_name="fact_payment",
            )
        )

        # Sales-value checks
        results.extend(
            check_non_negative_columns(
                dataframe=fact_sales,
                table_name="fact_sales",
                columns=[
                    "gross_amount",
                    "discount_amount",
                    "tax_amount",
                    "net_amount",
                ],
            )
        )

        if "quantity" in fact_sales.columns:
            results.append(
                check_positive_column(
                    dataframe=fact_sales,
                    table_name="fact_sales",
                    column_name="quantity",
                )
            )

        results.append(
            check_sales_amount_equation(
                dataframe=fact_sales,
            )
        )

        # Payment-value checks
        results.extend(
            check_non_negative_columns(
                dataframe=fact_payment,
                table_name="fact_payment",
                columns=[
                    "payment_amount",
                    "refund_amount",
                ],
            )
        )

        results.append(
            check_payment_amount_equation(
                dataframe=fact_payment,
            )
        )

        # SCD Type 2 checks
        results.append(
            check_current_scd_uniqueness(
                dataframe=dim_customer,
                table_name="dim_customer",
                business_key="customer_id",
            )
        )

        results.append(
            check_current_scd_uniqueness(
                dataframe=dim_product,
                table_name="dim_product",
                business_key="product_id",
            )
        )

        results.append(
            check_scd_date_range(
                dataframe=dim_customer,
                table_name="dim_customer",
            )
        )

        results.append(
            check_scd_date_range(
                dataframe=dim_product,
                table_name="dim_product",
            )
        )

        # Date-key consistency
        results.append(
            check_date_key_consistency(
                dataframe=fact_sales,
                table_name="fact_sales",
                date_column="order_date",
                date_key_column=(
                    "order_date_key"
                ),
            )
        )

        results.append(
            check_date_key_consistency(
                dataframe=fact_payment,
                table_name="fact_payment",
                date_column="payment_date",
                date_key_column=(
                    "payment_date_key"
                ),
            )
        )

        results.append(
            check_current_scd_end_date(
                dataframe=dim_customer,
                table_name="dim_customer",
            )
        )

        results.append(
            check_current_scd_end_date(
                dataframe=dim_product,
                table_name="dim_product",
            )
        )

        for result in results:
            print_check_result(result)

        passed_count = sum(
            1
            for result in results
            if result["status"] == "PASS"
        )

        failed_count = sum(
            1
            for result in results
            if result["status"] == "FAIL"
        )

        audit_output_path = (
            write_quality_audit(
                spark=spark,
                results=results,
                batch_date=batch_date,
            )
        )

        print("=" * 125)
        print("GOLD DATA QUALITY SUMMARY")
        print(f"Total checks: {len(results)}")
        print(f"Passed:       {passed_count}")
        print(f"Failed:       {failed_count}")
        print(
            f"Audit output: {audit_output_path}"
        )
        print("=" * 125)

        if failed_count > 0:
            raise ValueError(
                "Gold data quality checks failed. "
                f"Failed checks: {failed_count}"
            )

        print(
            "GOLD DATA QUALITY CHECKS COMPLETED "
            "SUCCESSFULLY"
        )

    finally:
        for dataframe in cached_dataframes:
            dataframe.unpersist()

        spark.stop()