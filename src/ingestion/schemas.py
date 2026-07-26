from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("customer_segment", StringType(), True),
    ]
)


PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock_quantity", IntegerType(), True),
    ]
)


ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("shipping_city", StringType(), True),
        StructField("shipping_state", StringType(), True),
    ]
)


ORDER_ITEM_SCHEMA = StructType(
    [
        StructField("order_item_id", StringType(), False),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_percentage", DoubleType(), True),
    ]
)


PAYMENT_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), False),
        StructField("order_id", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", DoubleType(), True),
        StructField("payment_date", StringType(), True),
    ]
)


DATASET_SCHEMAS = {
    "customers": CUSTOMER_SCHEMA,
    "products": PRODUCT_SCHEMA,
    "orders": ORDER_SCHEMA,
    "order_items": ORDER_ITEM_SCHEMA,
    "payments": PAYMENT_SCHEMA,
}


DATASET_PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "order_items": ["order_item_id"],
    "payments": ["payment_id"],
}