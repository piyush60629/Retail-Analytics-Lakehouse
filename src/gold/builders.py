from __future__ import annotations

from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_parquet(
    spark: SparkSession,
    path: Path,
    description: str,
) -> DataFrame:
    """
    Read a Parquet dataset and return a clear error
    when the expected path does not exist.
    """

    if not path.exists():
        raise FileNotFoundError(
            f"{description} not found: {path}"
        )

    return spark.read.parquet(str(path))


def read_scd2_dimension(
    spark: SparkSession,
    root: Path,
    batch_date: str,
    dataset_name: str,
) -> DataFrame:
    path = (
        root
        / f"batch_date={batch_date}"
        / dataset_name
    )

    return read_parquet(
        spark=spark,
        path=path,
        description=f"SCD2 {dataset_name}",
    )


def read_enriched_fact(
    spark: SparkSession,
    root: Path,
    batch_date: str,
    dataset_name: str,
) -> DataFrame:
    path = (
        root
        / f"batch_date={batch_date}"
        / dataset_name
    )

    return read_parquet(
        spark=spark,
        path=path,
        description=f"Enriched fact {dataset_name}",
    )


def select_existing_columns(
    dataframe: DataFrame,
    columns: Iterable[str],
) -> DataFrame:
    """
    Select only columns that are present.

    This keeps the Gold builder compatible with minor
    schema differences in the source datasets.
    """

    existing_columns = [
        column_name
        for column_name in columns
        if column_name in dataframe.columns
    ]

    return dataframe.select(*existing_columns)


def add_dimension_surrogate_key(
    dataframe: DataFrame,
    business_key: str,
    surrogate_key_name: str,
) -> DataFrame:
    """
    Generate the same deterministic surrogate key used
    by fact enrichment.
    """

    return dataframe.withColumn(
        surrogate_key_name,
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(
                    F.col(business_key).cast("string"),
                    F.lit("NULL"),
                ),
                F.coalesce(
                    F.col("effective_from").cast("string"),
                    F.lit("NULL"),
                ),
                F.coalesce(
                    F.col("record_version").cast("string"),
                    F.lit("NULL"),
                ),
            ),
            256,
        ),
    )


def build_customer_dimension(
    customers: DataFrame,
) -> DataFrame:
    """
    Create the Gold customer dimension.
    """

    customers_with_key = add_dimension_surrogate_key(
        dataframe=customers,
        business_key="customer_id",
        surrogate_key_name="customer_sk",
    )

    preferred_columns = [
        "customer_sk",
        "customer_id",
        "customer_name",
        "first_name",
        "last_name",
        "email",
        "phone",
        "gender",
        "date_of_birth",
        "city",
        "state",
        "country",
        "postal_code",
        "customer_segment",
        "registration_date",
        "effective_from",
        "effective_to",
        "is_current",
        "record_version",
        "scd_initial_load_date",
        "record_hash",
        "batch_date",
        "ingestion_timestamp",
    ]

    result = select_existing_columns(
        dataframe=customers_with_key,
        columns=preferred_columns,
    )

    return result.withColumn(
        "gold_created_timestamp",
        F.current_timestamp(),
    )


def build_product_dimension(
    products: DataFrame,
) -> DataFrame:
    """
    Create the Gold product dimension.
    """

    products_with_key = add_dimension_surrogate_key(
        dataframe=products,
        business_key="product_id",
        surrogate_key_name="product_sk",
    )

    preferred_columns = [
        "product_sk",
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "supplier",
        "unit_price",
        "cost_price",
        "stock_quantity",
        "product_status",
        "effective_from",
        "effective_to",
        "is_current",
        "record_version",
        "scd_initial_load_date",
        "record_hash",
        "batch_date",
        "ingestion_timestamp",
    ]

    result = select_existing_columns(
        dataframe=products_with_key,
        columns=preferred_columns,
    )

    return result.withColumn(
        "gold_created_timestamp",
        F.current_timestamp(),
    )


def find_date_column(
    dataframe: DataFrame,
    candidates: list[str],
    dataset_name: str,
) -> str:
    """
    Return the first available transaction-date column.
    """

    for candidate in candidates:
        if candidate in dataframe.columns:
            return candidate

    raise ValueError(
        f"No transaction date column found for "
        f"{dataset_name}. Tried {candidates}. "
        f"Available columns: {dataframe.columns}"
    )


def add_date_key(
    dataframe: DataFrame,
    date_column: str,
    date_key_name: str,
) -> DataFrame:
    """
    Add an integer date key in yyyyMMdd format.
    """

    return dataframe.withColumn(
        date_key_name,
        F.date_format(
            F.to_date(F.col(date_column)),
            "yyyyMMdd",
        ).cast("int"),
    )


def build_order_fact(
    orders: DataFrame,
) -> DataFrame:
    """
    Create the Gold order fact.
    """

    order_date_column = find_date_column(
        dataframe=orders,
        candidates=[
            "order_date",
            "order_timestamp",
            "order_datetime",
            "created_at",
        ],
        dataset_name="orders",
    )

    result = add_date_key(
        dataframe=orders,
        date_column=order_date_column,
        date_key_name="order_date_key",
    )

    preferred_columns = [
        "order_sk",
        "order_id",
        "customer_sk",
        "customer_id",
        "order_date_key",
        order_date_column,
        "order_status",
        "sales_channel",
        "shipping_method",
        "shipping_city",
        "shipping_state",
        "shipping_country",
        "subtotal",
        "discount_amount",
        "tax_amount",
        "shipping_amount",
        "total_amount",
        "order_amount",
        "currency",
        "batch_date",
        "record_hash",
        "ingestion_timestamp",
    ]

    result = select_existing_columns(
        dataframe=result,
        columns=preferred_columns,
    )

    return (
        result
        .withColumn(
            "order_count",
            F.lit(1),
        )
        .withColumn(
            "gold_created_timestamp",
            F.current_timestamp(),
        )
    )


def build_order_item_fact(
    order_items: DataFrame,
) -> DataFrame:
    """
    Create the Gold order-item fact.
    """

    preferred_columns = [
        "order_item_id",
        "order_sk",
        "order_id",
        "product_sk",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "tax_amount",
        "line_amount",
        "total_amount",
        "batch_date",
        "record_hash",
        "ingestion_timestamp",
    ]

    result = select_existing_columns(
        dataframe=order_items,
        columns=preferred_columns,
    )

    if (
        "quantity" in result.columns
        and "unit_price" in result.columns
        and "gross_line_amount" not in result.columns
    ):
        result = result.withColumn(
            "gross_line_amount",
            F.col("quantity")
            * F.col("unit_price"),
        )

    return (
        result
        .withColumn(
            "order_item_count",
            F.lit(1),
        )
        .withColumn(
            "gold_created_timestamp",
            F.current_timestamp(),
        )
    )


def build_payment_fact(
    payments: DataFrame,
) -> DataFrame:
    """
    Create the Gold payment fact.
    """

    payment_date_column = None

    for candidate in [
        "payment_date",
        "payment_timestamp",
        "payment_datetime",
        "created_at",
    ]:
        if candidate in payments.columns:
            payment_date_column = candidate
            break

    result = payments

    if payment_date_column:
        result = add_date_key(
            dataframe=result,
            date_column=payment_date_column,
            date_key_name="payment_date_key",
        )

    preferred_columns = [
        "payment_id",
        "order_sk",
        "order_id",
        "payment_date_key",
        payment_date_column,
        "payment_method",
        "payment_status",
        "transaction_id",
        "payment_amount",
        "amount",
        "currency",
        "batch_date",
        "record_hash",
        "ingestion_timestamp",
    ]

    preferred_columns = [
        column_name
        for column_name in preferred_columns
        if column_name is not None
    ]

    result = select_existing_columns(
        dataframe=result,
        columns=preferred_columns,
    )

    return (
        result
        .withColumn(
            "payment_count",
            F.lit(1),
        )
        .withColumn(
            "gold_created_timestamp",
            F.current_timestamp(),
        )
    )


def build_date_dimension(
    orders: DataFrame,
    payments: DataFrame,
) -> DataFrame:
    """
    Build a date dimension from order and payment dates.
    """

    date_dataframes = []

    order_date_column = find_date_column(
        dataframe=orders,
        candidates=[
            "order_date",
            "order_timestamp",
            "order_datetime",
            "created_at",
        ],
        dataset_name="orders",
    )

    order_dates = orders.select(
        F.to_date(
            F.col(order_date_column)
        ).alias("full_date")
    )

    date_dataframes.append(order_dates)

    for candidate in [
        "payment_date",
        "payment_timestamp",
        "payment_datetime",
        "created_at",
    ]:
        if candidate in payments.columns:
            payment_dates = payments.select(
                F.to_date(
                    F.col(candidate)
                ).alias("full_date")
            )

            date_dataframes.append(payment_dates)
            break

    combined_dates = date_dataframes[0]

    for dataframe in date_dataframes[1:]:
        combined_dates = combined_dates.unionByName(
            dataframe
        )

    return (
        combined_dates
        .filter(F.col("full_date").isNotNull())
        .dropDuplicates(["full_date"])
        .withColumn(
            "date_key",
            F.date_format(
                F.col("full_date"),
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn(
            "day_of_month",
            F.dayofmonth("full_date"),
        )
        .withColumn(
            "day_name",
            F.date_format("full_date", "EEEE"),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("full_date"),
        )
        .withColumn(
            "week_of_year",
            F.weekofyear("full_date"),
        )
        .withColumn(
            "month_number",
            F.month("full_date"),
        )
        .withColumn(
            "month_name",
            F.date_format("full_date", "MMMM"),
        )
        .withColumn(
            "quarter_number",
            F.quarter("full_date"),
        )
        .withColumn(
            "year_number",
            F.year("full_date"),
        )
        .withColumn(
            "year_month",
            F.date_format("full_date", "yyyy-MM"),
        )
        .withColumn(
            "is_weekend",
            F.dayofweek("full_date").isin([1, 7]),
        )
        .withColumn(
            "gold_created_timestamp",
            F.current_timestamp(),
        )
        .select(
            "date_key",
            "full_date",
            "day_of_month",
            "day_name",
            "day_of_week",
            "week_of_year",
            "month_number",
            "month_name",
            "quarter_number",
            "year_number",
            "year_month",
            "is_weekend",
            "gold_created_timestamp",
        )
    )


def validate_unique_key(
    dataframe: DataFrame,
    key_columns: list[str],
    table_name: str,
) -> None:
    """
    Ensure a Gold table has no duplicate primary keys.
    """

    missing_columns = [
        column_name
        for column_name in key_columns
        if column_name not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{table_name} is missing key columns: "
            f"{missing_columns}"
        )

    duplicate_count = (
        dataframe
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"{table_name} contains "
            f"{duplicate_count} duplicate keys."
        )


def validate_not_null(
    dataframe: DataFrame,
    column_name: str,
    table_name: str,
) -> None:
    """
    Ensure an important warehouse key is not null.
    """

    if column_name not in dataframe.columns:
        raise ValueError(
            f"{table_name} does not contain "
            f"required column {column_name}."
        )

    null_count = (
        dataframe
        .filter(F.col(column_name).isNull())
        .count()
    )

    if null_count > 0:
        raise ValueError(
            f"{table_name} contains "
            f"{null_count} null values in "
            f"{column_name}."
        )


def write_gold_table(
    dataframe: DataFrame,
    gold_root: Path,
    batch_date: str,
    table_name: str,
) -> Path:
    """
    Write a Gold table as Parquet.
    """

    output_path = (
        gold_root
        / f"batch_date={batch_date}"
        / table_name
    )

    (
        dataframe.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    return output_path