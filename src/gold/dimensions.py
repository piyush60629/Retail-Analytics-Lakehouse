from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def existing_columns(
    dataframe: DataFrame,
    columns: List[str],
) -> List[str]:
    """
    Return only columns that exist in the dataframe.
    """

    return [
        column_name
        for column_name in columns
        if column_name in dataframe.columns
    ]


def add_customer_surrogate_key(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Generate the same deterministic customer surrogate key
    used during fact enrichment.

    One customer can have multiple surrogate keys because
    each SCD Type 2 version represents a different historical
    dimension record.
    """

    required_columns = [
        "customer_id",
        "effective_from",
        "record_version",
    ]

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "Customer SCD2 dataset is missing columns "
            f"required for customer_sk: {missing_columns}"
        )

    return dataframe.withColumn(
        "customer_sk",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(
                    F.col("customer_id").cast("string"),
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


def build_dim_customer(
    customers: DataFrame,
    batch_date: str,
) -> DataFrame:
    """
    Build the Gold customer dimension from the SCD Type 2
    customer snapshot.

    Grain:
        One row per customer history version.
    """

    dataframe = customers

    if "customer_sk" not in dataframe.columns:
        dataframe = add_customer_surrogate_key(
            dataframe
        )

    selected_columns = existing_columns(
        dataframe,
        [
            "customer_sk",
            "customer_id",
            "customer_full_name",
            "customer_name",
            "first_name",
            "last_name",
            "email",
            "phone",
            "gender",
            "city",
            "state",
            "country",
            "postal_code",
            "customer_segment",
            "customer_status",
            "status",
            "signup_date",
            "registration_date",
            "date_of_birth",
            "effective_from",
            "effective_to",
            "is_current",
            "record_version",
            "scd_initial_load_date",
            "record_hash",
            "batch_date",
            "ingestion_timestamp",
        ],
    )

    dataframe = dataframe.select(
        *selected_columns
    )

    status_column = None

    if "customer_status" in dataframe.columns:
        status_column = "customer_status"
    elif "status" in dataframe.columns:
        status_column = "status"

    if status_column:
        dataframe = dataframe.withColumn(
            "is_active",
            F.when(
                F.upper(
                    F.col(status_column)
                ).isin(
                    "ACTIVE",
                    "ENABLED",
                    "CURRENT",
                ),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
    elif "is_current" in dataframe.columns:
        dataframe = dataframe.withColumn(
            "is_active",
            F.col("is_current"),
        )
    else:
        dataframe = dataframe.withColumn(
            "is_active",
            F.lit(True),
        )

    return (
        dataframe
        .withColumn(
            "_gold_batch_date",
            F.to_date(F.lit(batch_date)),
        )
        .withColumn(
            "_gold_processed_at",
            F.current_timestamp(),
        )
        .withColumn(
            "_record_source",
            F.lit("SCD2_CUSTOMERS"),
        )
    )


def add_product_surrogate_key(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Generate the same deterministic product surrogate key
    used during fact enrichment.
    """

    required_columns = [
        "product_id",
        "effective_from",
        "record_version",
    ]

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "Product SCD2 dataset is missing columns "
            f"required for product_sk: {missing_columns}"
        )

    return dataframe.withColumn(
        "product_sk",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(
                    F.col("product_id").cast("string"),
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


def build_dim_product(
    products: DataFrame,
    batch_date: str,
) -> DataFrame:
    """
    Build the Gold product dimension from the SCD Type 2
    product snapshot.

    Grain:
        One row per product history version.
    """

    dataframe = products

    if "product_sk" not in dataframe.columns:
        dataframe = add_product_surrogate_key(
            dataframe
        )

    selected_columns = existing_columns(
        dataframe,
        [
            "product_sk",
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "supplier",
            "price",
            "unit_price",
            "cost_price",
            "stock_quantity",
            "quantity_available",
            "product_status",
            "status",
            "effective_from",
            "effective_to",
            "is_current",
            "record_version",
            "scd_initial_load_date",
            "record_hash",
            "batch_date",
            "ingestion_timestamp",
        ],
    )

    dataframe = dataframe.select(
        *selected_columns
    )

    price_column = None

    if "price" in dataframe.columns:
        price_column = "price"
    elif "unit_price" in dataframe.columns:
        price_column = "unit_price"

    if (
        price_column
        and "cost_price" in dataframe.columns
    ):
        dataframe = dataframe.withColumn(
            "unit_margin",
            F.round(
                F.col(price_column)
                - F.col("cost_price"),
                2,
            ),
        )

        dataframe = dataframe.withColumn(
            "margin_percentage",
            F.when(
                F.col(price_column) > 0,
                F.round(
                    (
                        F.col("unit_margin")
                        / F.col(price_column)
                    )
                    * 100,
                    2,
                ),
            ),
        )

    return (
        dataframe
        .withColumn(
            "_gold_batch_date",
            F.to_date(F.lit(batch_date)),
        )
        .withColumn(
            "_gold_processed_at",
            F.current_timestamp(),
        )
        .withColumn(
            "_record_source",
            F.lit("SCD2_PRODUCTS"),
        )
    )


def build_dim_date(
    spark: SparkSession,
    orders: DataFrame,
    payments: DataFrame,
    batch_date: str,
) -> DataFrame:
    """
    Create a continuous date dimension using the minimum and
    maximum available order and payment dates.
    """

    date_dataframes = []

    if "order_day" in orders.columns:
        date_dataframes.append(
            orders.select(
                F.to_date(
                    F.col("order_day")
                ).alias("calendar_date")
            )
        )
    elif "order_date" in orders.columns:
        date_dataframes.append(
            orders.select(
                F.to_date(
                    F.col("order_date")
                ).alias("calendar_date")
            )
        )
    elif "order_timestamp" in orders.columns:
        date_dataframes.append(
            orders.select(
                F.to_date(
                    F.col("order_timestamp")
                ).alias("calendar_date")
            )
        )
    elif "order_datetime" in orders.columns:
        date_dataframes.append(
            orders.select(
                F.to_date(
                    F.col("order_datetime")
                ).alias("calendar_date")
            )
        )

    if "payment_day" in payments.columns:
        date_dataframes.append(
            payments.select(
                F.to_date(
                    F.col("payment_day")
                ).alias("calendar_date")
            )
        )
    elif "payment_date" in payments.columns:
        date_dataframes.append(
            payments.select(
                F.to_date(
                    F.col("payment_date")
                ).alias("calendar_date")
            )
        )
    elif "payment_timestamp" in payments.columns:
        date_dataframes.append(
            payments.select(
                F.to_date(
                    F.col("payment_timestamp")
                ).alias("calendar_date")
            )
        )
    elif "transaction_date" in payments.columns:
        date_dataframes.append(
            payments.select(
                F.to_date(
                    F.col("transaction_date")
                ).alias("calendar_date")
            )
        )

    if not date_dataframes:
        raise ValueError(
            "No supported order or payment date column "
            "was found."
        )

    combined_dates = date_dataframes[0]

    for dataframe in date_dataframes[1:]:
        combined_dates = (
            combined_dates.unionByName(
                dataframe
            )
        )

    date_limits = (
        combined_dates
        .filter(
            F.col("calendar_date").isNotNull()
        )
        .agg(
            F.min("calendar_date").alias(
                "minimum_date"
            ),
            F.max("calendar_date").alias(
                "maximum_date"
            ),
        )
        .collect()[0]
    )

    minimum_date = date_limits["minimum_date"]
    maximum_date = date_limits["maximum_date"]

    if (
        minimum_date is None
        or maximum_date is None
    ):
        raise ValueError(
            "Date dimension cannot be generated because "
            "all source dates are null."
        )

    date_range = spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{minimum_date}'),
                to_date('{maximum_date}'),
                interval 1 day
            )
        ) AS calendar_date
        """
    )

    return (
        date_range
        .withColumn(
            "date_key",
            F.date_format(
                F.col("calendar_date"),
                "yyyyMMdd",
            ).cast("integer"),
        )
        .withColumn(
            "day_number",
            F.dayofmonth("calendar_date"),
        )
        .withColumn(
            "day_name",
            F.date_format(
                F.col("calendar_date"),
                "EEEE",
            ),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("calendar_date"),
        )
        .withColumn(
            "week_of_year",
            F.weekofyear("calendar_date"),
        )
        .withColumn(
            "month_number",
            F.month("calendar_date"),
        )
        .withColumn(
            "month_name",
            F.date_format(
                F.col("calendar_date"),
                "MMMM",
            ),
        )
        .withColumn(
            "quarter_number",
            F.quarter("calendar_date"),
        )
        .withColumn(
            "quarter_name",
            F.concat(
                F.lit("Q"),
                F.quarter("calendar_date"),
            ),
        )
        .withColumn(
            "year_number",
            F.year("calendar_date"),
        )
        .withColumn(
            "year_month",
            F.date_format(
                F.col("calendar_date"),
                "yyyy-MM",
            ),
        )
        .withColumn(
            "is_weekend",
            F.dayofweek(
                "calendar_date"
            ).isin(1, 7),
        )
        .withColumn(
            "_gold_batch_date",
            F.to_date(F.lit(batch_date)),
        )
        .withColumn(
            "_gold_processed_at",
            F.current_timestamp(),
        )
        .withColumn(
            "_record_source",
            F.lit("GENERATED_DATE_RANGE"),
        )
        .select(
            "date_key",
            "calendar_date",
            "day_number",
            "day_name",
            "day_of_week",
            "week_of_year",
            "month_number",
            "month_name",
            "quarter_number",
            "quarter_name",
            "year_number",
            "year_month",
            "is_weekend",
            "_gold_batch_date",
            "_gold_processed_at",
            "_record_source",
        )
    )