from __future__ import annotations

from typing import Iterable, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_required_columns(
    dataframe: DataFrame,
    required_columns: Iterable[str],
    dataset_name: str,
) -> None:
    """
    Raise an error when required columns are missing.
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


def find_existing_column(
    dataframe: DataFrame,
    candidate_columns: Iterable[str],
) -> Optional[str]:
    """
    Return the first available column from a list of
    possible column names.
    """

    for column_name in candidate_columns:
        if column_name in dataframe.columns:
            return column_name

    return None


def current_dimension_records(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Return current SCD Type 2 dimension records.

    If is_current is not available, return all records.
    """

    if "is_current" not in dataframe.columns:
        return dataframe

    return dataframe.filter(
        F.col("is_current").cast("boolean")
        == F.lit(True)
    )


def safe_round(
    column_expression,
    scale: int = 2,
):
    """
    Round a Spark numeric expression.
    """

    return F.round(
        column_expression,
        scale,
    )


def build_daily_sales_summary(
    fact_sales: DataFrame,
) -> DataFrame:
    """
    Build daily sales KPIs.

    Grain:
        One row per order date.
    """

    validate_required_columns(
        dataframe=fact_sales,
        required_columns=[
            "order_date",
            "order_sk",
            "customer_sk",
            "quantity",
            "gross_amount",
            "discount_amount",
            "tax_amount",
            "net_amount",
        ],
        dataset_name="fact_sales",
    )

    return (
        fact_sales
        .groupBy(
            F.to_date(
                F.col("order_date")
            ).alias("order_date")
        )
        .agg(
            F.countDistinct(
                "order_sk"
            ).alias("total_orders"),

            F.sum(
                F.col("quantity")
            ).alias("total_items"),

            safe_round(
                F.sum(
                    F.col("gross_amount")
                )
            ).alias("gross_revenue"),

            safe_round(
                F.sum(
                    F.col("discount_amount")
                )
            ).alias("total_discount"),

            safe_round(
                F.sum(
                    F.col("tax_amount")
                )
            ).alias("total_tax"),

            safe_round(
                F.sum(
                    F.col("net_amount")
                )
            ).alias("net_revenue"),

            F.countDistinct(
                "customer_sk"
            ).alias("unique_customers"),
        )
        .withColumn(
            "average_order_value",
            safe_round(
                F.when(
                    F.col("total_orders") > 0,
                    F.col("net_revenue")
                    / F.col("total_orders"),
                ).otherwise(
                    F.lit(0.0)
                )
            ),
        )
        .select(
            "order_date",
            "total_orders",
            "total_items",
            "gross_revenue",
            "total_discount",
            "total_tax",
            "net_revenue",
            "average_order_value",
            "unique_customers",
        )
        .orderBy("order_date")
    )


def build_product_performance(
    fact_sales: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """
    Build product-level performance KPIs.

    Grain:
        One row per product surrogate key.
    """

    validate_required_columns(
        dataframe=fact_sales,
        required_columns=[
            "product_sk",
            "order_sk",
            "quantity",
            "gross_amount",
            "net_amount",
        ],
        dataset_name="fact_sales",
    )

    validate_required_columns(
        dataframe=dim_product,
        required_columns=[
            "product_sk",
            "product_id",
        ],
        dataset_name="dim_product",
    )

    current_products = current_dimension_records(
        dim_product
    )

    product_name_column = find_existing_column(
        current_products,
        [
            "product_name",
            "name",
            "product_title",
        ],
    )

    category_column = find_existing_column(
        current_products,
        [
            "category",
            "product_category",
            "category_name",
        ],
    )

    dimension_columns: List = [
        F.col("product_sk"),
        F.col("product_id"),
    ]

    if product_name_column:
        dimension_columns.append(
            F.col(product_name_column).alias(
                "product_name"
            )
        )
    else:
        dimension_columns.append(
            F.lit(None)
            .cast("string")
            .alias("product_name")
        )

    if category_column:
        dimension_columns.append(
            F.col(category_column).alias(
                "category"
            )
        )
    else:
        dimension_columns.append(
            F.lit(None)
            .cast("string")
            .alias("category")
        )

    product_lookup = (
        current_products
        .select(*dimension_columns)
        .dropDuplicates(
            ["product_sk"]
        )
    )

    product_metrics = (
        fact_sales
        .groupBy("product_sk")
        .agg(
            F.sum(
                F.col("quantity")
            ).alias(
                "total_quantity_sold"
            ),

            F.countDistinct(
                "order_sk"
            ).alias("total_orders"),

            safe_round(
                F.sum(
                    F.col("gross_amount")
                )
            ).alias("gross_revenue"),

            safe_round(
                F.sum(
                    F.col("net_amount")
                )
            ).alias("net_revenue"),
        )
        .withColumn(
            "average_selling_price",
            safe_round(
                F.when(
                    F.col(
                        "total_quantity_sold"
                    ) > 0,
                    F.col("net_revenue")
                    / F.col(
                        "total_quantity_sold"
                    ),
                ).otherwise(
                    F.lit(0.0)
                )
            ),
        )
    )

    return (
        product_metrics
        .join(
            product_lookup,
            on="product_sk",
            how="left",
        )
        .select(
            "product_sk",
            "product_id",
            "product_name",
            "category",
            "total_quantity_sold",
            "total_orders",
            "gross_revenue",
            "net_revenue",
            "average_selling_price",
        )
        .orderBy(
            F.col("net_revenue").desc()
        )
    )


def build_customer_performance(
    fact_sales: DataFrame,
    dim_customer: DataFrame,
) -> DataFrame:
    """
    Build customer-level performance KPIs.

    Grain:
        One row per customer surrogate key.
    """

    validate_required_columns(
        dataframe=fact_sales,
        required_columns=[
            "customer_sk",
            "order_sk",
            "quantity",
            "net_amount",
            "order_date",
        ],
        dataset_name="fact_sales",
    )

    validate_required_columns(
        dataframe=dim_customer,
        required_columns=[
            "customer_sk",
            "customer_id",
        ],
        dataset_name="dim_customer",
    )

    current_customers = current_dimension_records(
        dim_customer
    )

    customer_name_column = find_existing_column(
        current_customers,
        [
            "customer_name",
            "full_name",
            "name",
        ],
    )

    first_name_column = find_existing_column(
        current_customers,
        [
            "first_name",
            "customer_first_name",
        ],
    )

    last_name_column = find_existing_column(
        current_customers,
        [
            "last_name",
            "customer_last_name",
        ],
    )

    if customer_name_column:
        customer_name_expression = F.col(
            customer_name_column
        )
    elif (
        first_name_column
        and last_name_column
    ):
        customer_name_expression = F.concat_ws(
            " ",
            F.col(first_name_column),
            F.col(last_name_column),
        )
    elif first_name_column:
        customer_name_expression = F.col(
            first_name_column
        )
    else:
        customer_name_expression = (
            F.lit(None).cast("string")
        )

    customer_lookup = (
        current_customers
        .select(
            F.col("customer_sk"),
            F.col("customer_id"),
            customer_name_expression.alias(
                "customer_name"
            ),
        )
        .dropDuplicates(
            ["customer_sk"]
        )
    )

    customer_metrics = (
        fact_sales
        .groupBy("customer_sk")
        .agg(
            F.countDistinct(
                "order_sk"
            ).alias("total_orders"),

            F.sum(
                F.col("quantity")
            ).alias("total_items"),

            safe_round(
                F.sum(
                    F.col("net_amount")
                )
            ).alias("total_spent"),

            F.min(
                F.to_date(
                    F.col("order_date")
                )
            ).alias("first_order_date"),

            F.max(
                F.to_date(
                    F.col("order_date")
                )
            ).alias("latest_order_date"),
        )
        .withColumn(
            "average_order_value",
            safe_round(
                F.when(
                    F.col("total_orders") > 0,
                    F.col("total_spent")
                    / F.col("total_orders"),
                ).otherwise(
                    F.lit(0.0)
                )
            ),
        )
    )

    return (
        customer_metrics
        .join(
            customer_lookup,
            on="customer_sk",
            how="left",
        )
        .select(
            "customer_sk",
            "customer_id",
            "customer_name",
            "total_orders",
            "total_items",
            "total_spent",
            "average_order_value",
            "first_order_date",
            "latest_order_date",
        )
        .orderBy(
            F.col("total_spent").desc()
        )
    )


def build_payment_summary(
    fact_payment: DataFrame,
) -> DataFrame:
    """
    Build daily payment-method and payment-status KPIs.

    Grain:
        One row per payment date, method, and status.
    """

    validate_required_columns(
        dataframe=fact_payment,
        required_columns=[
            "payment_date",
            "payment_amount",
            "refund_amount",
            "net_payment_amount",
        ],
        dataset_name="fact_payment",
    )

    payment_method_column = find_existing_column(
        fact_payment,
        [
            "payment_method",
            "method",
            "payment_type",
        ],
    )

    payment_status_column = find_existing_column(
        fact_payment,
        [
            "payment_status",
            "status",
        ],
    )

    prepared_dataframe = fact_payment

    if payment_method_column:
        prepared_dataframe = (
            prepared_dataframe
            .withColumn(
                "_payment_method",
                F.col(payment_method_column),
            )
        )
    else:
        prepared_dataframe = (
            prepared_dataframe
            .withColumn(
                "_payment_method",
                F.lit("UNKNOWN"),
            )
        )

    if payment_status_column:
        prepared_dataframe = (
            prepared_dataframe
            .withColumn(
                "_payment_status",
                F.col(payment_status_column),
            )
        )
    else:
        prepared_dataframe = (
            prepared_dataframe
            .withColumn(
                "_payment_status",
                F.lit("UNKNOWN"),
            )
        )

    return (
        prepared_dataframe
        .groupBy(
            F.to_date(
                F.col("payment_date")
            ).alias("payment_date"),

            F.col(
                "_payment_method"
            ).alias("payment_method"),

            F.col(
                "_payment_status"
            ).alias("payment_status"),
        )
        .agg(
            F.count(
                F.lit(1)
            ).alias("payment_count"),

            safe_round(
                F.sum(
                    F.col("payment_amount")
                )
            ).alias("payment_amount"),

            safe_round(
                F.sum(
                    F.col("refund_amount")
                )
            ).alias("refund_amount"),

            safe_round(
                F.sum(
                    F.col(
                        "net_payment_amount"
                    )
                )
            ).alias(
                "net_payment_amount"
            ),
        )
        .orderBy(
            "payment_date",
            "payment_method",
            "payment_status",
        )
    )


def build_executive_kpis(
    fact_sales: DataFrame,
    fact_payment: DataFrame,
    batch_date: str,
) -> DataFrame:
    """
    Build a one-row executive KPI summary.
    """

    validate_required_columns(
        dataframe=fact_sales,
        required_columns=[
            "order_sk",
            "customer_sk",
            "product_sk",
            "quantity",
            "gross_amount",
            "discount_amount",
            "tax_amount",
            "net_amount",
        ],
        dataset_name="fact_sales",
    )

    validate_required_columns(
        dataframe=fact_payment,
        required_columns=[
            "payment_amount",
            "refund_amount",
            "net_payment_amount",
        ],
        dataset_name="fact_payment",
    )

    sales_kpis = (
        fact_sales
        .agg(
            F.countDistinct(
                "order_sk"
            ).alias("total_orders"),

            F.countDistinct(
                "customer_sk"
            ).alias("total_customers"),

            F.countDistinct(
                "product_sk"
            ).alias(
                "total_products_sold"
            ),

            F.sum(
                F.col("quantity")
            ).alias("total_items_sold"),

            safe_round(
                F.sum(
                    F.col("gross_amount")
                )
            ).alias("gross_revenue"),

            safe_round(
                F.sum(
                    F.col("discount_amount")
                )
            ).alias("total_discount"),

            safe_round(
                F.sum(
                    F.col("tax_amount")
                )
            ).alias("total_tax"),

            safe_round(
                F.sum(
                    F.col("net_amount")
                )
            ).alias("total_revenue"),
        )
        .withColumn(
            "average_order_value",
            safe_round(
                F.when(
                    F.col("total_orders") > 0,
                    F.col("total_revenue")
                    / F.col("total_orders"),
                ).otherwise(
                    F.lit(0.0)
                )
            ),
        )
    )

    payment_kpis = (
        fact_payment
        .agg(
            safe_round(
                F.sum(
                    F.col("payment_amount")
                )
            ).alias(
                "total_payment_amount"
            ),

            safe_round(
                F.sum(
                    F.col("refund_amount")
                )
            ).alias("total_refunds"),

            safe_round(
                F.sum(
                    F.col(
                        "net_payment_amount"
                    )
                )
            ).alias(
                "net_collected_amount"
            ),
        )
    )

    return (
        sales_kpis
        .crossJoin(payment_kpis)
        .withColumn(
            "batch_date",
            F.to_date(
                F.lit(batch_date)
            ),
        )
        .select(
            "batch_date",
            "total_orders",
            "total_customers",
            "total_products_sold",
            "total_items_sold",
            "gross_revenue",
            "total_discount",
            "total_tax",
            "total_revenue",
            "average_order_value",
            "total_payment_amount",
            "total_refunds",
            "net_collected_amount",
        )
    )