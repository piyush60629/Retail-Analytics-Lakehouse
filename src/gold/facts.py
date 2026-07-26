from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


MONEY_TYPE = DecimalType(18, 2)


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


def validate_required_columns(
    dataframe: DataFrame,
    required_columns: List[str],
    dataset_name: str,
) -> None:
    """
    Validate that all required columns exist.
    """

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            f"{missing_columns}"
        )


def find_column(
    dataframe: DataFrame,
    candidate_columns: List[str],
) -> str | None:
    """
    Return the first available column from a list of
    possible column names.
    """

    for column_name in candidate_columns:
        if column_name in dataframe.columns:
            return column_name

    return None


def prepare_order_items(
    order_items: DataFrame,
) -> DataFrame:
    """
    Create standard sales amount columns when they do not
    already exist.

    Expected grain:
        One row per order item.
    """

    dataframe = order_items

    quantity_column = find_column(
        dataframe,
        [
            "quantity",
            "order_quantity",
            "item_quantity",
        ],
    )

    price_column = find_column(
        dataframe,
        [
            "unit_price",
            "price",
            "selling_price",
        ],
    )

    if "gross_amount" not in dataframe.columns:
        if quantity_column and price_column:
            dataframe = dataframe.withColumn(
                "gross_amount",
                F.round(
                    F.col(quantity_column).cast(MONEY_TYPE)
                    * F.col(price_column).cast(MONEY_TYPE),
                    2,
                ).cast(MONEY_TYPE),
            )
        else:
            raise ValueError(
                "Order items cannot calculate gross_amount. "
                "No quantity and price columns were found."
            )
    else:
        dataframe = dataframe.withColumn(
            "gross_amount",
            F.col("gross_amount").cast(MONEY_TYPE),
        )

    if "discount_amount" not in dataframe.columns:
        dataframe = dataframe.withColumn(
            "discount_amount",
            F.lit(0).cast(MONEY_TYPE),
        )
    else:
        dataframe = dataframe.withColumn(
            "discount_amount",
            F.coalesce(
                F.col("discount_amount").cast(MONEY_TYPE),
                F.lit(0).cast(MONEY_TYPE),
            ),
        )

    if "tax_amount" not in dataframe.columns:
        dataframe = dataframe.withColumn(
            "tax_amount",
            F.lit(0).cast(MONEY_TYPE),
        )
    else:
        dataframe = dataframe.withColumn(
            "tax_amount",
            F.coalesce(
                F.col("tax_amount").cast(MONEY_TYPE),
                F.lit(0).cast(MONEY_TYPE),
            ),
        )

    if "net_amount" not in dataframe.columns:
        dataframe = dataframe.withColumn(
            "net_amount",
            F.round(
                F.col("gross_amount")
                - F.col("discount_amount")
                + F.col("tax_amount"),
                2,
            ).cast(MONEY_TYPE),
        )
    else:
        dataframe = dataframe.withColumn(
            "net_amount",
            F.col("net_amount").cast(MONEY_TYPE),
        )

    return dataframe


def build_fact_sales(
    orders: DataFrame,
    order_items: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
    batch_date: str,
) -> DataFrame:
    """
    Build the Gold sales fact table.

    Grain:
        One row per order item.

    Important:
        customer_sk, product_sk and order_sk are expected to
        come from the Fact Enrichment layer.

        dim_customer and dim_product remain in the function
        signature for compatibility with the existing runner,
        but no surrogate-key lookup is performed here.
    """

    del dim_customer
    del dim_product

    validate_required_columns(
        dataframe=orders,
        required_columns=[
            "order_id",
            "order_sk",
            "customer_sk",
        ],
        dataset_name="Enriched orders",
    )

    validate_required_columns(
        dataframe=order_items,
        required_columns=[
            "order_id",
            "order_sk",
            "product_sk",
        ],
        dataset_name="Enriched order_items",
    )

    prepared_items = prepare_order_items(
        order_items
    ).alias("items")

    orders_df = orders.alias("orders")

    order_date_column = find_column(
        orders,
        [
            "order_day",
            "order_date",
            "order_timestamp",
            "order_datetime",
            "created_at",
        ],
    )

    if order_date_column is None:
        raise ValueError(
            "Enriched orders does not contain a supported "
            "order date column."
        )

    order_date_expression = F.to_date(
        F.col(f"orders.{order_date_column}")
    )

    order_status_column = find_column(
        orders,
        [
            "order_status",
            "status",
        ],
    )

    quantity_column = find_column(
        prepared_items,
        [
            "quantity",
            "order_quantity",
            "item_quantity",
        ],
    )

    unit_price_column = find_column(
        prepared_items,
        [
            "unit_price",
            "price",
            "selling_price",
        ],
    )

    dataframe = (
        prepared_items
        .join(
            orders_df,
            (
                F.col("items.order_id")
                == F.col("orders.order_id")
            )
            & (
                F.col("items.order_sk")
                == F.col("orders.order_sk")
            ),
            "inner",
        )
    )

    if "order_item_id" in order_items.columns:
        sales_sk_expression = F.sha2(
            F.concat_ws(
                "||",
                F.col(
                    "items.order_item_id"
                ).cast("string"),
                F.col(
                    "items.order_sk"
                ).cast("string"),
                F.col(
                    "items.product_sk"
                ).cast("string"),
            ),
            256,
        )
    else:
        sales_sk_expression = F.sha2(
            F.concat_ws(
                "||",
                F.col(
                    "items.order_sk"
                ).cast("string"),
                F.col(
                    "items.product_sk"
                ).cast("string"),
                F.monotonically_increasing_id().cast(
                    "string"
                ),
            ),
            256,
        )

    expressions = [
        sales_sk_expression.alias("sales_sk"),
        F.col("items.order_sk").alias("order_sk"),
        F.col("orders.customer_sk").alias(
            "customer_sk"
        ),
        F.col("items.product_sk").alias(
            "product_sk"
        ),
        F.col("items.order_id").alias("order_id"),
        F.date_format(
            order_date_expression,
            "yyyyMMdd",
        ).cast("integer").alias("order_date_key"),
        order_date_expression.alias("order_date"),
    ]

    if "order_item_id" in order_items.columns:
        expressions.append(
            F.col("items.order_item_id").alias(
                "order_item_id"
            )
        )

    if "product_id" in order_items.columns:
        expressions.append(
            F.col("items.product_id").alias(
                "product_id"
            )
        )

    if "customer_id" in orders.columns:
        expressions.append(
            F.col("orders.customer_id").alias(
                "customer_id"
            )
        )

    if quantity_column:
        expressions.append(
            F.col(
                f"items.{quantity_column}"
            ).alias("quantity")
        )

    if unit_price_column:
        expressions.append(
            F.col(
                f"items.{unit_price_column}"
            )
            .cast(MONEY_TYPE)
            .alias("unit_price")
        )

    expressions.extend(
        [
            F.col("items.gross_amount").alias(
                "gross_amount"
            ),
            F.col("items.discount_amount").alias(
                "discount_amount"
            ),
            F.col("items.tax_amount").alias(
                "tax_amount"
            ),
            F.col("items.net_amount").alias(
                "net_amount"
            ),
        ]
    )

    if order_status_column:
        expressions.append(
            F.col(
                f"orders.{order_status_column}"
            ).alias("order_status")
        )

    optional_order_columns = [
        "sales_channel",
        "channel",
        "shipping_method",
        "shipping_city",
        "shipping_state",
        "shipping_country",
        "currency",
    ]

    for column_name in optional_order_columns:
        if column_name in orders.columns:
            expressions.append(
                F.col(
                    f"orders.{column_name}"
                ).alias(column_name)
            )

    dataframe = dataframe.select(*expressions)

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
            F.lit(
                "ENRICHED_ORDERS_AND_ORDER_ITEMS"
            ),
        )
    )


def build_fact_payment(
    payments: DataFrame,
    orders: DataFrame,
    dim_customer: DataFrame,
    batch_date: str,
) -> DataFrame:
    """
    Build the Gold payment fact table.

    Grain:
        One row per payment transaction.

    Important:
        order_sk is expected to come from the enriched
        payments dataset.

        customer_sk is taken from enriched orders.

        dim_customer remains in the function signature for
        compatibility with the existing runner.
    """

    del dim_customer

    validate_required_columns(
        dataframe=payments,
        required_columns=[
            "payment_id",
            "order_id",
            "order_sk",
        ],
        dataset_name="Enriched payments",
    )

    validate_required_columns(
        dataframe=orders,
        required_columns=[
            "order_id",
            "order_sk",
            "customer_sk",
        ],
        dataset_name="Enriched orders",
    )

    payments_df = payments.alias("payments")
    orders_df = orders.alias("orders")

    dataframe = (
        payments_df
        .join(
            orders_df,
            (
                F.col("payments.order_id")
                == F.col("orders.order_id")
            )
            & (
                F.col("payments.order_sk")
                == F.col("orders.order_sk")
            ),
            "left",
        )
    )

    payment_date_column = find_column(
        payments,
        [
            "payment_day",
            "payment_date",
            "payment_timestamp",
            "transaction_date",
            "created_at",
        ],
    )

    if payment_date_column is None:
        raise ValueError(
            "Enriched payments does not contain a "
            "supported payment date column."
        )

    payment_date_expression = F.to_date(
        F.col(
            f"payments.{payment_date_column}"
        )
    )

    amount_column = find_column(
        payments,
        [
            "payment_amount",
            "amount",
            "transaction_amount",
        ],
    )

    if amount_column is None:
        raise ValueError(
            "Enriched payments does not contain a "
            "payment amount column."
        )

    payment_status_column = find_column(
        payments,
        [
            "payment_status",
            "transaction_status",
            "status",
        ],
    )

    payment_method_column = find_column(
        payments,
        [
            "payment_method",
            "payment_type",
            "method",
        ],
    )

    expressions = [
        F.sha2(
            F.concat_ws(
                "||",
                F.col(
                    "payments.payment_id"
                ).cast("string"),
                F.col(
                    "payments.order_sk"
                ).cast("string"),
            ),
            256,
        ).alias("payment_sk"),
        F.col("payments.payment_id").alias(
            "payment_id"
        ),
        F.col("payments.order_sk").alias(
            "order_sk"
        ),
        F.col("orders.customer_sk").alias(
            "customer_sk"
        ),
        F.col("payments.order_id").alias(
            "order_id"
        ),
        F.date_format(
            payment_date_expression,
            "yyyyMMdd",
        ).cast("integer").alias(
            "payment_date_key"
        ),
        payment_date_expression.alias(
            "payment_date"
        ),
        F.col(
            f"payments.{amount_column}"
        )
        .cast(MONEY_TYPE)
        .alias("payment_amount"),
    ]

    if "customer_id" in orders.columns:
        expressions.append(
            F.col("orders.customer_id").alias(
                "customer_id"
            )
        )

    if payment_method_column:
        expressions.append(
            F.col(
                f"payments.{payment_method_column}"
            ).alias("payment_method")
        )

    if payment_status_column:
        expressions.append(
            F.col(
                f"payments.{payment_status_column}"
            ).alias("payment_status")
        )

    optional_payment_columns = [
        "transaction_id",
        "refund_amount",
        "currency",
        "gateway",
        "payment_provider",
        "failure_reason",
    ]

    for column_name in optional_payment_columns:
        if column_name in payments.columns:
            expression = F.col(
                f"payments.{column_name}"
            )

            if column_name == "refund_amount":
                expression = expression.cast(
                    MONEY_TYPE
                )

            expressions.append(
                expression.alias(column_name)
            )

    dataframe = dataframe.select(*expressions)

    if "payment_status" in dataframe.columns:
        dataframe = dataframe.withColumn(
            "is_successful",
            F.upper(
                F.trim(
                    F.col("payment_status")
                )
            ).isin(
                "SUCCESS",
                "SUCCESSFUL",
                "COMPLETED",
                "PAID",
                "CAPTURED",
            ),
        )
    else:
        dataframe = dataframe.withColumn(
            "is_successful",
            F.lit(None).cast("boolean"),
        )

    if "refund_amount" not in dataframe.columns:
        dataframe = dataframe.withColumn(
            "refund_amount",
            F.lit(0).cast(MONEY_TYPE),
        )
    else:
        dataframe = dataframe.withColumn(
            "refund_amount",
            F.coalesce(
                F.col("refund_amount"),
                F.lit(0).cast(MONEY_TYPE),
            ),
        )

    dataframe = dataframe.withColumn(
        "net_payment_amount",
        F.round(
            F.col("payment_amount")
            - F.col("refund_amount"),
            2,
        ).cast(MONEY_TYPE),
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
            F.lit(
                "ENRICHED_PAYMENTS_AND_ORDERS"
            ),
        )
    )