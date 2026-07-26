from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from src.common.storage_config import (
    join_storage_path,
)

from pyspark.sql import functions as F

from src.fact_enrichment.config import (
    ENRICHED_FACT_ROOT,
    FACT_ENRICHMENT_AUDIT_ROOT,
    SCD2_ROOT,
    SILVER_ROOT,
)
from src.fact_enrichment.lookup import (
    add_dimension_surrogate_key,
    add_order_surrogate_key,
    find_date_column,
    historical_dimension_lookup,
    read_scd2_dimension,
    read_silver_fact,
    validate_no_missing_surrogate_keys,
    validate_unique_fact_key,
    write_enriched_fact,
)
from src.silver.spark_session import (
    create_spark_session,
)


def enrich_orders(
    spark,
    batch_date: str,
):
    """
    Add customer_sk and order_sk to orders.
    """

    orders = read_silver_fact(
        spark=spark,
        silver_root=SILVER_ROOT,
        batch_date=batch_date,
        dataset_name="orders",
    ).cache()

    customers = read_scd2_dimension(
        spark=spark,
        scd2_root=SCD2_ROOT,
        batch_date=batch_date,
        dataset_name="customers",
    )

    customers = add_dimension_surrogate_key(
        dimension=customers,
        business_keys=["customer_id"],
        surrogate_key_name="customer_sk",
    )

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

    enriched_orders = historical_dimension_lookup(
        fact_dataframe=orders,
        dimension_dataframe=customers,
        fact_business_key="customer_id",
        dimension_business_key="customer_id",
        fact_date_column=order_date_column,
        surrogate_key_column="customer_sk",
        fact_primary_key="order_id",
    )

    enriched_orders = add_order_surrogate_key(
        enriched_orders
    ).cache()

    from src.fact_enrichment.lookup import (
        print_missing_lookup_examples,
    )

    print_missing_lookup_examples(
    dataframe=enriched_orders,
    surrogate_key_column="customer_sk",
    selected_columns=[
        "order_id",
        "customer_id",
        order_date_column,
    ],
    dataset_name="orders",
)

    validate_no_missing_surrogate_keys(
        dataframe=enriched_orders,
        surrogate_key_column="customer_sk",
        dataset_name="orders",
    )

    validate_unique_fact_key(
        dataframe=enriched_orders,
        primary_keys=["order_id"],
        dataset_name="orders",
    )

    source_count = orders.count()
    output_count = enriched_orders.count()

    if source_count != output_count:
        raise ValueError(
            "Orders count changed during enrichment: "
            f"source={source_count}, output={output_count}"
        )

    output_path = write_enriched_fact(
        dataframe=enriched_orders,
        output_root=ENRICHED_FACT_ROOT,
        batch_date=batch_date,
        dataset_name="orders",
    )

    audit = {
        "dataset_name": "orders",
        "source_record_count": source_count,
        "output_record_count": output_count,
        "missing_customer_sk_count": 0,
        "output_path": str(output_path),
        "status": "SUCCESS",
    }

    orders.unpersist()

    return enriched_orders, audit


def enrich_order_items(
    spark,
    batch_date: str,
    enriched_orders,
):
    """
    Add product_sk and order_sk to order items.
    """

    order_items = read_silver_fact(
        spark=spark,
        silver_root=SILVER_ROOT,
        batch_date=batch_date,
        dataset_name="order_items",
    ).cache()

    products = read_scd2_dimension(
        spark=spark,
        scd2_root=SCD2_ROOT,
        batch_date=batch_date,
        dataset_name="products",
    )

    products = add_dimension_surrogate_key(
        dimension=products,
        business_keys=["product_id"],
        surrogate_key_name="product_sk",
    )

    order_date_column = find_date_column(
        dataframe=enriched_orders,
        candidates=[
            "order_date",
            "order_timestamp",
            "order_datetime",
            "created_at",
        ],
        dataset_name="orders",
    )

    order_context = enriched_orders.select(
        "order_id",
        "order_sk",
        F.col(order_date_column).alias(
            "_order_event_date"
        ),
    )

    items_with_order_context = order_items.join(
        order_context,
        on="order_id",
        how="left",
    )

    enriched_items = historical_dimension_lookup(
        fact_dataframe=items_with_order_context,
        dimension_dataframe=products,
        fact_business_key="product_id",
        dimension_business_key="product_id",
        fact_date_column="_order_event_date",
        surrogate_key_column="product_sk",
        fact_primary_key="order_item_id",
    ).drop("_order_event_date")

    enriched_items = enriched_items.cache()

    validate_no_missing_surrogate_keys(
        dataframe=enriched_items,
        surrogate_key_column="order_sk",
        dataset_name="order_items",
    )

    validate_no_missing_surrogate_keys(
        dataframe=enriched_items,
        surrogate_key_column="product_sk",
        dataset_name="order_items",
    )

    validate_unique_fact_key(
        dataframe=enriched_items,
        primary_keys=["order_item_id"],
        dataset_name="order_items",
    )

    source_count = order_items.count()
    output_count = enriched_items.count()

    if source_count != output_count:
        raise ValueError(
            "Order-items count changed during enrichment: "
            f"source={source_count}, output={output_count}"
        )

    output_path = write_enriched_fact(
        dataframe=enriched_items,
        output_root=ENRICHED_FACT_ROOT,
        batch_date=batch_date,
        dataset_name="order_items",
    )

    audit = {
        "dataset_name": "order_items",
        "source_record_count": source_count,
        "output_record_count": output_count,
        "missing_order_sk_count": 0,
        "missing_product_sk_count": 0,
        "output_path": str(output_path),
        "status": "SUCCESS",
    }

    order_items.unpersist()
    enriched_items.unpersist()

    return audit


def enrich_payments(
    spark,
    batch_date: str,
    enriched_orders,
):
    """
    Add order_sk to payments.
    """

    payments = read_silver_fact(
        spark=spark,
        silver_root=SILVER_ROOT,
        batch_date=batch_date,
        dataset_name="payments",
    ).cache()

    order_lookup = enriched_orders.select(
        "order_id",
        "order_sk",
    ).dropDuplicates(["order_id"])

    enriched_payments = (
        payments
        .join(
            order_lookup,
            on="order_id",
            how="left",
        )
        .cache()
    )

    validate_no_missing_surrogate_keys(
        dataframe=enriched_payments,
        surrogate_key_column="order_sk",
        dataset_name="payments",
    )

    payment_primary_keys = (
        ["payment_id"]
        if "payment_id" in enriched_payments.columns
        else ["order_id"]
    )

    validate_unique_fact_key(
        dataframe=enriched_payments,
        primary_keys=payment_primary_keys,
        dataset_name="payments",
    )

    source_count = payments.count()
    output_count = enriched_payments.count()

    if source_count != output_count:
        raise ValueError(
            "Payments count changed during enrichment: "
            f"source={source_count}, output={output_count}"
        )

    output_path = write_enriched_fact(
        dataframe=enriched_payments,
        output_root=ENRICHED_FACT_ROOT,
        batch_date=batch_date,
        dataset_name="payments",
    )

    audit = {
        "dataset_name": "payments",
        "source_record_count": source_count,
        "output_record_count": output_count,
        "missing_order_sk_count": 0,
        "output_path": str(output_path),
        "status": "SUCCESS",
    }

    payments.unpersist()
    enriched_payments.unpersist()

    return audit


def write_audit(
    spark,
    batch_date: str,
    audit_records: list[dict[str, Any]],
) -> str:
    """
    Write fact enrichment audit using Spark.
    Compatible with Local + ADLS.
    """

    output_path = join_storage_path(
        FACT_ENRICHMENT_AUDIT_ROOT,
        f"batch_date={batch_date}",
    )

    payload = {
        "batch_date": batch_date,
        "audit_timestamp": datetime.now().isoformat(),
        "datasets": audit_records,
    }

    audit_df = spark.createDataFrame(
        [(json.dumps(payload),)],
        ["json_payload"],
    )

    (
        audit_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .text(output_path)
    )

    return output_path


def run_fact_enrichment(
    batch_date: str,
) -> None:
    """
    Enrich Silver facts with warehouse surrogate keys.
    """

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    audit_records = []

    try:
        print("=" * 125)
        print(
            f"FACT SURROGATE KEY ENRICHMENT — "
            f"BATCH DATE: {batch_date}"
        )
        print("=" * 125)

        enriched_orders, orders_audit = enrich_orders(
            spark=spark,
            batch_date=batch_date,
        )

        audit_records.append(orders_audit)

        order_items_audit = enrich_order_items(
            spark=spark,
            batch_date=batch_date,
            enriched_orders=enriched_orders,
        )

        audit_records.append(order_items_audit)

        payments_audit = enrich_payments(
            spark=spark,
            batch_date=batch_date,
            enriched_orders=enriched_orders,
        )

        audit_records.append(payments_audit)

        audit_path = write_audit(
            spark=spark,
            batch_date=batch_date,
            audit_records=audit_records,
        )

        print("=" * 125)

        for audit in audit_records:
            print(
                f"{audit['dataset_name']:<12} | "
                f"Source: "
                f"{audit['source_record_count']:>8,} | "
                f"Output: "
                f"{audit['output_record_count']:>8,} | "
                f"Status: {audit['status']}"
            )

        print("=" * 125)
        print(
            "FACT SURROGATE KEY ENRICHMENT "
            "COMPLETED SUCCESSFULLY"
        )
        enriched_output_path = join_storage_path(
            ENRICHED_FACT_ROOT,
            f"batch_date={batch_date}",
        )

        print(f"Output: {enriched_output_path}")
        print(f"Audit: {audit_path}")
        print("=" * 125)

        enriched_orders.unpersist()

    finally:
        spark.stop()