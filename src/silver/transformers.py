from typing import Callable, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.silver.transformation_utils import (
    cast_date_column,
    cast_decimal_column,
    cast_integer_column,
    cast_timestamp_column,
    column_exists,
    finalize_silver_dataframe,
    lowercase_column,
    titlecase_column,
    trim_string_columns,
    uppercase_column,
)


def transform_customers(
    dataframe: DataFrame,
    batch_date: str,
) -> DataFrame:
    dataframe = trim_string_columns(dataframe)

    for column_name in [
        "first_name",
        "last_name",
        "city",
        "state",
        "country",
    ]:
        dataframe = titlecase_column(
            dataframe,
            column_name,
        )

    dataframe = lowercase_column(
        dataframe,
        "email",
    )

    for column_name in [
        "customer_status",
        "status",
    ]:
        dataframe = uppercase_column(
            dataframe,
            column_name,
        )

    for column_name in [
        "signup_date",
        "registration_date",
        "date_of_birth",
    ]:
        dataframe = cast_date_column(
            dataframe,
            column_name,
        )

    for column_name in [
        "created_at",
        "updated_at",
    ]:
        dataframe = cast_timestamp_column(
            dataframe,
            column_name,
        )

    if (
        column_exists(dataframe, "first_name")
        and column_exists(dataframe, "last_name")
    ):
        dataframe = dataframe.withColumn(
            "customer_full_name",
            F.concat_ws(
                " ",
                F.col("first_name"),
                F.col("last_name"),
            ),
        )

    return finalize_silver_dataframe(
        dataframe,
        batch_date,
    )


def transform_products(
    dataframe: DataFrame,
    batch_date: str,
) -> DataFrame:
    dataframe = trim_string_columns(dataframe)

    for column_name in [
        "product_name",
        "category",
        "subcategory",
        "brand",
    ]:
        dataframe = titlecase_column(
            dataframe,
            column_name,
        )

    for column_name in [
        "price",
        "unit_price",
        "cost_price",
    ]:
        dataframe = cast_decimal_column(
            dataframe,
            column_name,
        )

    for column_name in [
        "stock_quantity",
        "quantity_available",
    ]:
        dataframe = cast_integer_column(
            dataframe,
            column_name,
        )

    for column_name in [
        "created_at",
        "updated_at",
    ]:
        dataframe = cast_timestamp_column(
            dataframe,
            column_name,
        )

    for column_name in [
        "product_status",
        "status",
    ]:
        dataframe = uppercase_column(
            dataframe,
            column_name,
        )

    return finalize_silver_dataframe(
        dataframe,
        batch_date,
    )


def transform_orders(
    dataframe: DataFrame,
    batch_date: str,
) -> DataFrame:
    dataframe = trim_string_columns(dataframe)

    for column_name in [
        "order_status",
        "status",
    ]:
        dataframe = uppercase_column(
            dataframe,
            column_name,
        )

    for column_name in [
        "order_date",
        "created_at",
        "updated_at",
    ]:
        dataframe = cast_timestamp_column(
            dataframe,
            column_name,
        )

    for column_name in [
        "total_amount",
        "order_amount",
        "discount_amount",
        "tax_amount",
        "shipping_amount",
        "net_amount",
    ]:
        dataframe = cast_decimal_column(
            dataframe,
            column_name,
        )

    if column_exists(dataframe, "order_date"):
        dataframe = (
            dataframe
            .withColumn(
                "order_day",
                F.to_date(F.col("order_date")),
            )
            .withColumn(
                "order_year",
                F.year(F.col("order_date")),
            )
            .withColumn(
                "order_month",
                F.month(F.col("order_date")),
            )
            .withColumn(
                "order_quarter",
                F.quarter(F.col("order_date")),
            )
        )

    return finalize_silver_dataframe(
        dataframe,
        batch_date,
    )


def transform_order_items(
    dataframe: DataFrame,
    batch_date: str,
) -> DataFrame:
    dataframe = trim_string_columns(dataframe)

    dataframe = cast_integer_column(
        dataframe,
        "quantity",
    )

    for column_name in [
        "unit_price",
        "price",
        "discount_amount",
        "discount_percentage",
        "tax_amount",
        "line_amount",
        "gross_amount",
        "net_amount",
    ]:
        dataframe = cast_decimal_column(
            dataframe,
            column_name,
        )

    price_column = None

    if column_exists(dataframe, "unit_price"):
        price_column = "unit_price"
    elif column_exists(dataframe, "price"):
        price_column = "price"

    if (
        price_column
        and column_exists(dataframe, "quantity")
    ):
        dataframe = dataframe.withColumn(
            "gross_amount",
            F.round(
                F.col(price_column)
                * F.col("quantity"),
                2,
            ).cast(DecimalType(18, 2)),
        )

    if (
        column_exists(dataframe, "gross_amount")
        and column_exists(
            dataframe,
            "discount_amount",
        )
    ):
        dataframe = dataframe.withColumn(
            "net_amount",
            F.round(
                F.col("gross_amount")
                - F.coalesce(
                    F.col("discount_amount"),
                    F.lit(0),
                ),
                2,
            ).cast(DecimalType(18, 2)),
        )

    elif column_exists(dataframe, "gross_amount"):
        dataframe = dataframe.withColumn(
            "net_amount",
            F.col("gross_amount"),
        )

    return finalize_silver_dataframe(
        dataframe,
        batch_date,
    )


def transform_payments(
    dataframe: DataFrame,
    batch_date: str,
) -> DataFrame:
    dataframe = trim_string_columns(dataframe)

    for column_name in [
        "payment_status",
        "status",
    ]:
        dataframe = uppercase_column(
            dataframe,
            column_name,
        )

    if column_exists(dataframe, "payment_method"):
        dataframe = dataframe.withColumn(
            "payment_method",
            F.initcap(
                F.regexp_replace(
                    F.lower(F.col("payment_method")),
                    "_",
                    " ",
                )
            ),
        )

    for column_name in [
        "payment_amount",
        "amount",
        "refund_amount",
    ]:
        dataframe = cast_decimal_column(
            dataframe,
            column_name,
        )

    for column_name in [
        "payment_date",
        "transaction_date",
        "created_at",
        "updated_at",
    ]:
        dataframe = cast_timestamp_column(
            dataframe,
            column_name,
        )

    payment_timestamp_column = None

    if column_exists(dataframe, "payment_date"):
        payment_timestamp_column = "payment_date"

    elif column_exists(
        dataframe,
        "transaction_date",
    ):
        payment_timestamp_column = "transaction_date"

    if payment_timestamp_column:
        dataframe = dataframe.withColumn(
            "payment_day",
            F.to_date(
                F.col(payment_timestamp_column)
            ),
        )

    return finalize_silver_dataframe(
        dataframe,
        batch_date,
    )


TRANSFORMERS: Dict[
    str,
    Callable[[DataFrame, str], DataFrame],
] = {
    "customers": transform_customers,
    "products": transform_products,
    "orders": transform_orders,
    "order_items": transform_order_items,
    "payments": transform_payments,
}