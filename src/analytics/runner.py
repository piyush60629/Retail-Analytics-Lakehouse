from __future__ import annotations

from datetime import datetime

from src.common.spark_storage import (
    spark_path_exists,
)
from src.common.storage_config import (
    join_storage_path,
)

from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from src.analytics.config import (
    ANALYTICS_AUDIT_ROOT,
    ANALYTICS_ROOT,
    GOLD_ROOT,
)
from src.analytics.kpis import (
    build_customer_performance,
    build_daily_sales_summary,
    build_executive_kpis,
    build_payment_summary,
    build_product_performance,
)
from src.silver.spark_session import (
    create_spark_session,
)


def read_gold_table(
    spark: SparkSession,
    table_name: str,
    batch_date: str,
) -> DataFrame:
    """
    Read a Gold table from local storage or ADLS.
    """

    input_path = join_storage_path(
        GOLD_ROOT,
        f"batch_date={batch_date}",
        table_name,
    )

    if not spark_path_exists(
        spark=spark,
        path=input_path,
    ):
        raise FileNotFoundError(
            f"Gold table not found: {input_path}"
        )

    return spark.read.parquet(
        input_path
    )


def write_analytics_table(
    dataframe: DataFrame,
    table_name: str,
    batch_date: str,
) -> Dict[str, object]:
    """
    Write one analytics table.
    """

    output_path = join_storage_path(
        ANALYTICS_ROOT,
        f"batch_date={batch_date}",
        table_name,
    )

    record_count = dataframe.count()

    (
        dataframe
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    return {
        "table_name": table_name,
        "record_count": record_count,
        "status": "SUCCESS",
        "output_path": output_path,
    }


def write_analytics_audit(
    spark: SparkSession,
    audit_records: List[Dict[str, object]],
    batch_date: str,
) -> str:
    """
    Write analytics execution metadata.
    """

    output_path = join_storage_path(
        ANALYTICS_AUDIT_ROOT,
        f"batch_date={batch_date}",
    )

    execution_timestamp = (
        datetime.now().isoformat(
            timespec="seconds"
        )
    )

    prepared_records = []

    for record in audit_records:
        prepared_records.append(
            {
                "batch_date": batch_date,
                "execution_timestamp": (
                    execution_timestamp
                ),
                "table_name": record[
                    "table_name"
                ],
                "record_count": int(
                    record["record_count"]
                ),
                "status": record["status"],
                "output_path": record[
                    "output_path"
                ],
            }
        )

    audit_dataframe = spark.createDataFrame(
        prepared_records
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


def print_result(
    result: Dict[str, object],
) -> None:
    """
    Print one analytics-table result.
    """

    print(
        f"{result['table_name']:<25} "
        f"Records: "
        f"{result['record_count']:>10} | "
        f"Status: {result['status']}"
    )


def run_analytics_pipeline(
    batch_date: str,
) -> None:
    """
    Build and write all analytics tables.
    """

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    cached_dataframes: List[DataFrame] = []
    audit_records: List[Dict[str, object]] = []

    try:
        print("=" * 115)
        print(
            "ANALYTICS AND KPI LAYER — "
            f"BATCH DATE: {batch_date}"
        )
        print("=" * 115)

        dim_customer = read_gold_table(
            spark=spark,
            table_name="dim_customer",
            batch_date=batch_date,
        ).cache()

        dim_product = read_gold_table(
            spark=spark,
            table_name="dim_product",
            batch_date=batch_date,
        ).cache()

        fact_sales = read_gold_table(
            spark=spark,
            table_name="fact_sales",
            batch_date=batch_date,
        ).cache()

        fact_payment = read_gold_table(
            spark=spark,
            table_name="fact_payment",
            batch_date=batch_date,
        ).cache()

        cached_dataframes.extend(
            [
                dim_customer,
                dim_product,
                fact_sales,
                fact_payment,
            ]
        )

        daily_sales_summary = (
            build_daily_sales_summary(
                fact_sales=fact_sales,
            )
        )

        product_performance = (
            build_product_performance(
                fact_sales=fact_sales,
                dim_product=dim_product,
            )
        )

        customer_performance = (
            build_customer_performance(
                fact_sales=fact_sales,
                dim_customer=dim_customer,
            )
        )

        payment_summary = (
            build_payment_summary(
                fact_payment=fact_payment,
            )
        )

        executive_kpis = (
            build_executive_kpis(
                fact_sales=fact_sales,
                fact_payment=fact_payment,
                batch_date=batch_date,
            )
        )

        analytics_tables = [
            (
                "daily_sales_summary",
                daily_sales_summary,
            ),
            (
                "product_performance",
                product_performance,
            ),
            (
                "customer_performance",
                customer_performance,
            ),
            (
                "payment_summary",
                payment_summary,
            ),
            (
                "executive_kpis",
                executive_kpis,
            ),
        ]

        for table_name, dataframe in analytics_tables:
            result = write_analytics_table(
                dataframe=dataframe,
                table_name=table_name,
                batch_date=batch_date,
            )

            audit_records.append(result)
            print_result(result)

        audit_output_path = (
            write_analytics_audit(
                spark=spark,
                audit_records=audit_records,
                batch_date=batch_date,
            )
        )

        print("=" * 115)
        print(
            "ANALYTICS AND KPI LAYER COMPLETED"
        )
        print(
            f"Analytics output: "
            f"{ANALYTICS_ROOT}"
        )
        print(
            f"Audit output: "
            f"{audit_output_path}"
        )
        print("=" * 115)

    finally:
        for dataframe in cached_dataframes:
            dataframe.unpersist()

        spark.stop()