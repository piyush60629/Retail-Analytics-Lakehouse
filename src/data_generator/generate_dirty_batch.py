from __future__ import annotations

import random
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = BASE_DIR / "data" / "raw"
INCOMING_DATA_DIR = BASE_DIR / "data" / "incoming"

RANDOM_SEED = 42
BATCH_DATE = "2026-07-21"

random.seed(RANDOM_SEED)


def load_source_data() -> dict[str, pd.DataFrame]:
    """Load all clean source datasets."""

    return {
        "customers": pd.read_csv(
            RAW_DATA_DIR / "customers" / "customers.csv"
        ),
        "products": pd.read_csv(
            RAW_DATA_DIR / "products" / "products.csv"
        ),
        "orders": pd.read_csv(
            RAW_DATA_DIR / "orders" / "orders.csv"
        ),
        "order_items": pd.read_csv(
            RAW_DATA_DIR / "order_items" / "order_items.csv"
        ),
        "payments": pd.read_csv(
            RAW_DATA_DIR / "payments" / "payments.csv"
        ),
    }


def sample_dataframe(
    dataframe: pd.DataFrame,
    number_of_rows: int,
) -> pd.DataFrame:
    """Return a deterministic sample from a dataframe."""

    sample_size = min(number_of_rows, len(dataframe))

    return dataframe.sample(
        n=sample_size,
        random_state=RANDOM_SEED,
    ).copy()


def inject_customer_issues(
    customers_df: pd.DataFrame,
) -> pd.DataFrame:
    """Introduce controlled customer data-quality issues."""

    dirty_df = customers_df.copy()

    # Missing emails
    missing_email_indexes = dirty_df.sample(
        n=10,
        random_state=1,
    ).index

    dirty_df.loc[
        missing_email_indexes,
        "email",
    ] = None

    # Invalid email formats
    invalid_email_indexes = dirty_df.sample(
        n=10,
        random_state=2,
    ).index

    dirty_df.loc[
        invalid_email_indexes,
        "email",
    ] = "invalid-email-format"

    # Missing city
    missing_city_indexes = dirty_df.sample(
        n=5,
        random_state=3,
    ).index

    dirty_df.loc[
        missing_city_indexes,
        "city",
    ] = None

    # Unexpected segment value
    invalid_segment_indexes = dirty_df.sample(
        n=5,
        random_state=4,
    ).index

    dirty_df.loc[
        invalid_segment_indexes,
        "customer_segment",
    ] = "Diamond"

    # Duplicate records
    duplicate_rows = dirty_df.sample(
        n=10,
        random_state=5,
    )

    dirty_df = pd.concat(
        [dirty_df, duplicate_rows],
        ignore_index=True,
    )

    return dirty_df


def inject_product_issues(
    products_df: pd.DataFrame,
) -> pd.DataFrame:
    """Introduce controlled product data-quality issues."""

    dirty_df = products_df.copy()

    # Negative prices
    negative_price_indexes = dirty_df.sample(
        n=5,
        random_state=6,
    ).index

    dirty_df.loc[
        negative_price_indexes,
        "price",
    ] = -999.99

    # Missing product names
    missing_name_indexes = dirty_df.sample(
        n=5,
        random_state=7,
    ).index

    dirty_df.loc[
        missing_name_indexes,
        "product_name",
    ] = None

    # Negative inventory
    negative_stock_indexes = dirty_df.sample(
        n=5,
        random_state=8,
    ).index

    dirty_df.loc[
        negative_stock_indexes,
        "stock_quantity",
    ] = -100

    # Invalid category
    invalid_category_indexes = dirty_df.sample(
        n=5,
        random_state=9,
    ).index

    dirty_df.loc[
        invalid_category_indexes,
        "category",
    ] = "UnknownCategory"

    # Duplicate products
    duplicate_rows = dirty_df.sample(
        n=5,
        random_state=10,
    )

    dirty_df = pd.concat(
        [dirty_df, duplicate_rows],
        ignore_index=True,
    )

    return dirty_df


def inject_order_issues(
    orders_df: pd.DataFrame,
) -> pd.DataFrame:
    """Introduce controlled order data-quality issues."""

    dirty_df = orders_df.copy()

    # Invalid customer references
    invalid_customer_indexes = dirty_df.sample(
        n=10,
        random_state=11,
    ).index

    dirty_df.loc[
        invalid_customer_indexes,
        "customer_id",
    ] = "CUST999999"

    # Missing shipping cities
    missing_city_indexes = dirty_df.sample(
        n=10,
        random_state=12,
    ).index

    dirty_df.loc[
        missing_city_indexes,
        "shipping_city",
    ] = None

    # Unexpected order statuses
    invalid_status_indexes = dirty_df.sample(
        n=10,
        random_state=13,
    ).index

    dirty_df.loc[
        invalid_status_indexes,
        "order_status",
    ] = "Unknown"

    # Invalid future order date
    future_date_indexes = dirty_df.sample(
        n=5,
        random_state=14,
    ).index

    dirty_df.loc[
        future_date_indexes,
        "order_date",
    ] = (
        datetime.now() + timedelta(days=365)
    ).isoformat()

    # Duplicate orders
    duplicate_rows = dirty_df.sample(
        n=10,
        random_state=15,
    )

    dirty_df = pd.concat(
        [dirty_df, duplicate_rows],
        ignore_index=True,
    )

    return dirty_df


def inject_order_item_issues(
    order_items_df: pd.DataFrame,
) -> pd.DataFrame:
    """Introduce controlled order-item issues."""

    dirty_df = order_items_df.copy()

    # Invalid product references
    invalid_product_indexes = dirty_df.sample(
        n=10,
        random_state=16,
    ).index

    dirty_df.loc[
        invalid_product_indexes,
        "product_id",
    ] = "PROD99999"

    # Invalid order references
    invalid_order_indexes = dirty_df.sample(
        n=10,
        random_state=17,
    ).index

    dirty_df.loc[
        invalid_order_indexes,
        "order_id",
    ] = "ORD99999999"

    # Zero quantities
    zero_quantity_indexes = dirty_df.sample(
        n=10,
        random_state=18,
    ).index

    dirty_df.loc[
        zero_quantity_indexes,
        "quantity",
    ] = 0

    # Negative prices
    negative_price_indexes = dirty_df.sample(
        n=10,
        random_state=19,
    ).index

    dirty_df.loc[
        negative_price_indexes,
        "unit_price",
    ] = -500

    # Invalid discount
    invalid_discount_indexes = dirty_df.sample(
        n=10,
        random_state=20,
    ).index

    dirty_df.loc[
        invalid_discount_indexes,
        "discount_percentage",
    ] = 150

    # Duplicate line items
    duplicate_rows = dirty_df.sample(
        n=10,
        random_state=21,
    )

    dirty_df = pd.concat(
        [dirty_df, duplicate_rows],
        ignore_index=True,
    )

    return dirty_df


def inject_payment_issues(
    payments_df: pd.DataFrame,
) -> pd.DataFrame:
    """Introduce controlled payment issues."""

    dirty_df = payments_df.copy()

    # Missing payment amounts
    missing_amount_indexes = dirty_df.sample(
        n=10,
        random_state=22,
    ).index

    dirty_df.loc[
        missing_amount_indexes,
        "payment_amount",
    ] = None

    # Negative payment amounts
    negative_amount_indexes = dirty_df.sample(
        n=10,
        random_state=23,
    ).index

    dirty_df.loc[
        negative_amount_indexes,
        "payment_amount",
    ] = -1000

    # Invalid order references
    invalid_order_indexes = dirty_df.sample(
        n=10,
        random_state=24,
    ).index

    dirty_df.loc[
        invalid_order_indexes,
        "order_id",
    ] = "ORD99999999"

    # Unexpected payment statuses
    invalid_status_indexes = dirty_df.sample(
        n=10,
        random_state=25,
    ).index

    dirty_df.loc[
        invalid_status_indexes,
        "payment_status",
    ] = "Unknown"

    # Invalid future payment date
    future_date_indexes = dirty_df.sample(
        n=5,
        random_state=26,
    ).index

    dirty_df.loc[
        future_date_indexes,
        "payment_date",
    ] = (
        datetime.now() + timedelta(days=365)
    ).isoformat()

    # Duplicate payments
    duplicate_rows = dirty_df.sample(
        n=10,
        random_state=27,
    )

    dirty_df = pd.concat(
        [dirty_df, duplicate_rows],
        ignore_index=True,
    )

    return dirty_df


def save_batch(
    dataset_name: str,
    dataframe: pd.DataFrame,
    batch_directory: Path,
) -> None:
    """Save one incoming batch file."""

    output_path = (
        batch_directory
        / f"{dataset_name}_{BATCH_DATE}.csv"
    )

    dataframe.to_csv(
        output_path,
        index=False,
    )

    print(
        f"Created: {output_path} "
        f"| Rows: {len(dataframe):,}"
    )


def main() -> None:
    print("=" * 70)
    print("DIRTY RETAIL DATA BATCH GENERATOR")
    print("=" * 70)

    batch_directory = (
        INCOMING_DATA_DIR / f"batch_date={BATCH_DATE}"
    )

    if batch_directory.exists():
        shutil.rmtree(batch_directory)

    batch_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    source_data = load_source_data()

    # Use smaller daily batches instead of copying all source data.
    customer_batch = sample_dataframe(
        source_data["customers"],
        1_000,
    )

    product_batch = sample_dataframe(
        source_data["products"],
        500,
    )

    order_batch = sample_dataframe(
        source_data["orders"],
        10_000,
    )

    order_ids = set(order_batch["order_id"])

    order_item_batch = source_data[
        "order_items"
    ][
        source_data["order_items"]["order_id"].isin(
            order_ids
        )
    ].copy()

    payment_batch = source_data[
        "payments"
    ][
        source_data["payments"]["order_id"].isin(
            order_ids
        )
    ].copy()

    dirty_customers = inject_customer_issues(
        customer_batch
    )

    dirty_products = inject_product_issues(
        product_batch
    )

    dirty_orders = inject_order_issues(
        order_batch
    )

    dirty_order_items = inject_order_item_issues(
        order_item_batch
    )

    dirty_payments = inject_payment_issues(
        payment_batch
    )

    print("\nSaving incoming batch...")

    save_batch(
        "customers",
        dirty_customers,
        batch_directory,
    )

    save_batch(
        "products",
        dirty_products,
        batch_directory,
    )

    save_batch(
        "orders",
        dirty_orders,
        batch_directory,
    )

    save_batch(
        "order_items",
        dirty_order_items,
        batch_directory,
    )

    save_batch(
        "payments",
        dirty_payments,
        batch_directory,
    )

    print("\n" + "=" * 70)
    print("DIRTY BATCH CREATED SUCCESSFULLY")
    print("=" * 70)

    print(f"\nBatch location: {batch_directory}")


if __name__ == "__main__":
    main()