from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.quality.validation_rules import (
    VALID_CUSTOMER_SEGMENTS,
    VALID_ORDER_STATUSES,
    VALID_PAYMENT_METHODS,
    VALID_PAYMENT_STATUSES,
    VALID_PRODUCT_CATEGORIES,
    initialise_validation_columns,
    split_valid_invalid,
    validate_allowed_values,
    validate_date_not_in_future,
    validate_duplicate_primary_keys,
    validate_email,
    validate_foreign_key,
    validate_null_values,
    validate_positive_number,
    validate_range,
    validate_required_columns,
)


BASE_DIR = Path(__file__).resolve().parents[2]

INCOMING_DATA_DIR = BASE_DIR / "data" / "incoming"
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
QUARANTINE_DATA_DIR = BASE_DIR / "data" / "quarantine"


CUSTOMER_REQUIRED_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "city",
    "state",
    "country",
    "signup_date",
    "customer_segment",
]

PRODUCT_REQUIRED_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "brand",
    "price",
    "stock_quantity",
]

ORDER_REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "order_status",
    "shipping_city",
    "shipping_state",
]

ORDER_ITEM_REQUIRED_COLUMNS = [
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_percentage",
]

PAYMENT_REQUIRED_COLUMNS = [
    "payment_id",
    "order_id",
    "payment_method",
    "payment_status",
    "payment_amount",
    "payment_date",
]


def load_csv(path: Path) -> pd.DataFrame:
    """Load a CSV file with basic error handling."""

    if not path.exists():
        raise FileNotFoundError(
            f"Input file was not found: {path}"
        )

    try:
        dataframe = pd.read_csv(path)
    except Exception as error:
        raise RuntimeError(
            f"Unable to read file {path}: {error}"
        ) from error

    return dataframe


def check_schema(
    dataframe: pd.DataFrame,
    required_columns: list[str],
    dataset_name: str,
) -> None:
    """Raise an error if required columns are missing."""

    missing_columns = validate_required_columns(
        dataframe,
        required_columns,
    )

    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            f"{missing_columns}"
        )


def validate_customers(
    dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate customer records."""

    check_schema(
        dataframe,
        CUSTOMER_REQUIRED_COLUMNS,
        "customers",
    )

    validated_df = initialise_validation_columns(
        dataframe
    )

    validate_null_values(
        validated_df,
        CUSTOMER_REQUIRED_COLUMNS,
    )

    validate_duplicate_primary_keys(
        validated_df,
        "customer_id",
    )

    validate_email(
        validated_df,
        "email",
    )

    validate_allowed_values(
        validated_df,
        "customer_segment",
        VALID_CUSTOMER_SEGMENTS,
    )

    validate_date_not_in_future(
        validated_df,
        "signup_date",
    )

    return split_valid_invalid(validated_df)


def validate_products(
    dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate product records."""

    check_schema(
        dataframe,
        PRODUCT_REQUIRED_COLUMNS,
        "products",
    )

    validated_df = initialise_validation_columns(
        dataframe
    )

    validate_null_values(
        validated_df,
        PRODUCT_REQUIRED_COLUMNS,
    )

    validate_duplicate_primary_keys(
        validated_df,
        "product_id",
    )

    validate_allowed_values(
        validated_df,
        "category",
        VALID_PRODUCT_CATEGORIES,
    )

    validate_positive_number(
        validated_df,
        "price",
    )

    validate_positive_number(
        validated_df,
        "stock_quantity",
        allow_zero=True,
    )

    return split_valid_invalid(validated_df)


def validate_orders(
    dataframe: pd.DataFrame,
    valid_customer_ids: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate order records."""

    check_schema(
        dataframe,
        ORDER_REQUIRED_COLUMNS,
        "orders",
    )

    validated_df = initialise_validation_columns(
        dataframe
    )

    validate_null_values(
        validated_df,
        ORDER_REQUIRED_COLUMNS,
    )

    validate_duplicate_primary_keys(
        validated_df,
        "order_id",
    )

    validate_allowed_values(
        validated_df,
        "order_status",
        VALID_ORDER_STATUSES,
    )

    validate_date_not_in_future(
        validated_df,
        "order_date",
    )

    validate_foreign_key(
        validated_df,
        "customer_id",
        valid_customer_ids,
    )

    return split_valid_invalid(validated_df)


def validate_order_items(
    dataframe: pd.DataFrame,
    valid_order_ids: set[str],
    valid_product_ids: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate order-item records."""

    check_schema(
        dataframe,
        ORDER_ITEM_REQUIRED_COLUMNS,
        "order_items",
    )

    validated_df = initialise_validation_columns(
        dataframe
    )

    validate_null_values(
        validated_df,
        ORDER_ITEM_REQUIRED_COLUMNS,
    )

    validate_duplicate_primary_keys(
        validated_df,
        "order_item_id",
    )

    validate_foreign_key(
        validated_df,
        "order_id",
        valid_order_ids,
    )

    validate_foreign_key(
        validated_df,
        "product_id",
        valid_product_ids,
    )

    validate_positive_number(
        validated_df,
        "quantity",
    )

    validate_positive_number(
        validated_df,
        "unit_price",
    )

    validate_range(
        validated_df,
        "discount_percentage",
        minimum=0,
        maximum=100,
    )

    return split_valid_invalid(validated_df)


def validate_payments(
    dataframe: pd.DataFrame,
    valid_order_ids: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate payment records."""

    check_schema(
        dataframe,
        PAYMENT_REQUIRED_COLUMNS,
        "payments",
    )

    validated_df = initialise_validation_columns(
        dataframe
    )

    validate_null_values(
        validated_df,
        PAYMENT_REQUIRED_COLUMNS,
    )

    validate_duplicate_primary_keys(
        validated_df,
        "payment_id",
    )

    validate_foreign_key(
        validated_df,
        "order_id",
        valid_order_ids,
    )

    validate_allowed_values(
        validated_df,
        "payment_method",
        VALID_PAYMENT_METHODS,
    )

    validate_allowed_values(
        validated_df,
        "payment_status",
        VALID_PAYMENT_STATUSES,
    )

    validate_positive_number(
        validated_df,
        "payment_amount",
    )

    validate_date_not_in_future(
        validated_df,
        "payment_date",
    )

    return split_valid_invalid(validated_df)


def save_dataframe(
    dataframe: pd.DataFrame,
    output_path: Path,
) -> None:
    """Save a dataframe to CSV."""

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    dataframe.to_csv(
        output_path,
        index=False,
    )


def build_audit_record(
    dataset_name: str,
    input_count: int,
    valid_count: int,
    invalid_count: int,
) -> dict[str, object]:
    """Create one dataset-level audit record."""

    validation_rate = (
        round(valid_count / input_count * 100, 2)
        if input_count > 0
        else 0
    )

    return {
        "dataset_name": dataset_name,
        "input_record_count": input_count,
        "valid_record_count": valid_count,
        "invalid_record_count": invalid_count,
        "validation_success_rate": validation_rate,
        "validation_timestamp": datetime.now().isoformat(),
    }


def process_dataset(
    dataset_name: str,
    input_dataframe: pd.DataFrame,
    valid_dataframe: pd.DataFrame,
    invalid_dataframe: pd.DataFrame,
    processed_directory: Path,
    quarantine_directory: Path,
    batch_date: str,
) -> dict[str, object]:
    """Save accepted and rejected records and return audit details."""

    valid_path = (
        processed_directory
        / f"{dataset_name}_{batch_date}.csv"
    )

    invalid_path = (
        quarantine_directory
        / f"{dataset_name}_rejected_{batch_date}.csv"
    )

    save_dataframe(
        valid_dataframe,
        valid_path,
    )

    save_dataframe(
        invalid_dataframe,
        invalid_path,
    )

    print(
        f"{dataset_name:<12} "
        f"Input: {len(input_dataframe):>7,} | "
        f"Valid: {len(valid_dataframe):>7,} | "
        f"Rejected: {len(invalid_dataframe):>5,}"
    )

    return build_audit_record(
        dataset_name=dataset_name,
        input_count=len(input_dataframe),
        valid_count=len(valid_dataframe),
        invalid_count=len(invalid_dataframe),
    )


def run_validation(batch_date: str) -> None:
    """Validate all datasets in an incoming batch."""

    incoming_batch_directory = (
        INCOMING_DATA_DIR
        / f"batch_date={batch_date}"
    )

    processed_batch_directory = (
        PROCESSED_DATA_DIR
        / f"batch_date={batch_date}"
    )

    quarantine_batch_directory = (
        QUARANTINE_DATA_DIR
        / f"batch_date={batch_date}"
    )

    if not incoming_batch_directory.exists():
        raise FileNotFoundError(
            "Incoming batch directory was not found: "
            f"{incoming_batch_directory}"
        )

    if processed_batch_directory.exists():
        shutil.rmtree(processed_batch_directory)

    if quarantine_batch_directory.exists():
        shutil.rmtree(quarantine_batch_directory)

    processed_batch_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    quarantine_batch_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("=" * 85)
    print(f"VALIDATING RETAIL BATCH: {batch_date}")
    print("=" * 85)

    customers_df = load_csv(
        incoming_batch_directory
        / f"customers_{batch_date}.csv"
    )

    products_df = load_csv(
        incoming_batch_directory
        / f"products_{batch_date}.csv"
    )

    orders_df = load_csv(
        incoming_batch_directory
        / f"orders_{batch_date}.csv"
    )

    order_items_df = load_csv(
        incoming_batch_directory
        / f"order_items_{batch_date}.csv"
    )

    payments_df = load_csv(
        incoming_batch_directory
        / f"payments_{batch_date}.csv"
    )

    valid_customers, invalid_customers = (
        validate_customers(customers_df)
    )

    valid_products, invalid_products = (
        validate_products(products_df)
    )

    valid_customer_ids = set(
        valid_customers["customer_id"]
    )

    valid_product_ids = set(
        valid_products["product_id"]
    )

    valid_orders, invalid_orders = validate_orders(
        orders_df,
        valid_customer_ids,
    )

    valid_order_ids = set(
        valid_orders["order_id"]
    )

    valid_order_items, invalid_order_items = (
        validate_order_items(
            order_items_df,
            valid_order_ids,
            valid_product_ids,
        )
    )

    valid_payments, invalid_payments = (
        validate_payments(
            payments_df,
            valid_order_ids,
        )
    )

    print("\nValidation results:")

    audit_records = []

    audit_records.append(
        process_dataset(
            "customers",
            customers_df,
            valid_customers,
            invalid_customers,
            processed_batch_directory,
            quarantine_batch_directory,
            batch_date,
        )
    )

    audit_records.append(
        process_dataset(
            "products",
            products_df,
            valid_products,
            invalid_products,
            processed_batch_directory,
            quarantine_batch_directory,
            batch_date,
        )
    )

    audit_records.append(
        process_dataset(
            "orders",
            orders_df,
            valid_orders,
            invalid_orders,
            processed_batch_directory,
            quarantine_batch_directory,
            batch_date,
        )
    )

    audit_records.append(
        process_dataset(
            "order_items",
            order_items_df,
            valid_order_items,
            invalid_order_items,
            processed_batch_directory,
            quarantine_batch_directory,
            batch_date,
        )
    )

    audit_records.append(
        process_dataset(
            "payments",
            payments_df,
            valid_payments,
            invalid_payments,
            processed_batch_directory,
            quarantine_batch_directory,
            batch_date,
        )
    )

    audit_path = (
        processed_batch_directory
        / f"validation_audit_{batch_date}.json"
    )

    with audit_path.open(
        "w",
        encoding="utf-8",
    ) as audit_file:
        json.dump(
            audit_records,
            audit_file,
            indent=4,
        )

    total_input = sum(
        record["input_record_count"]
        for record in audit_records
    )

    total_valid = sum(
        record["valid_record_count"]
        for record in audit_records
    )

    total_invalid = sum(
        record["invalid_record_count"]
        for record in audit_records
    )

    print("\n" + "-" * 85)
    print(f"Total input records:    {total_input:,}")
    print(f"Total accepted records: {total_valid:,}")
    print(f"Total rejected records: {total_invalid:,}")
    print("-" * 85)

    print(
        f"\nProcessed output: {processed_batch_directory}"
    )

    print(
        f"Quarantine output: {quarantine_batch_directory}"
    )

    print(f"Audit file: {audit_path}")

    print("\n" + "=" * 85)
    print("BATCH VALIDATION COMPLETED")
    print("=" * 85)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate an incoming retail data batch."
    )

    parser.add_argument(
        "--batch-date",
        required=True,
        help="Batch date in YYYY-MM-DD format.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    run_validation(
        batch_date=arguments.batch_date
    )


if __name__ == "__main__":
    main()