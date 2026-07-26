from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

CUSTOMERS_DIR = RAW_DATA_DIR / "customers"
PRODUCTS_DIR = RAW_DATA_DIR / "products"
ORDERS_DIR = RAW_DATA_DIR / "orders"
ORDER_ITEMS_DIR = RAW_DATA_DIR / "order_items"
PAYMENTS_DIR = RAW_DATA_DIR / "payments"


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

NUM_CUSTOMERS = 5_000
NUM_PRODUCTS = 1_000
NUM_ORDERS = 50_000

RANDOM_SEED = 42

fake = Faker("en_IN")
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)


CUSTOMER_SEGMENTS = [
    "Regular",
    "Silver",
    "Gold",
    "Platinum",
]

PRODUCT_CATEGORIES = [
    "Electronics",
    "Fashion",
    "Furniture",
    "Books",
    "Sports",
    "Beauty",
    "Home",
    "Toys",
]

BRANDS_BY_CATEGORY = {
    "Electronics": [
        "Samsung",
        "Sony",
        "Boat",
        "LG",
        "Philips",
        "Noise",
    ],
    "Fashion": [
        "Puma",
        "Adidas",
        "Levi's",
        "Allen Solly",
        "Roadster",
    ],
    "Furniture": [
        "Urban Ladder",
        "HomeTown",
        "Nilkamal",
        "IKEA",
        "Wakefit",
    ],
    "Books": [
        "Penguin",
        "HarperCollins",
        "Oxford",
        "Scholastic",
        "Pearson",
    ],
    "Sports": [
        "Nike",
        "Yonex",
        "Nivia",
        "Cosco",
        "Decathlon",
    ],
    "Beauty": [
        "Lakme",
        "Maybelline",
        "Mamaearth",
        "Nivea",
        "L'Oreal",
    ],
    "Home": [
        "Prestige",
        "Pigeon",
        "Milton",
        "Cello",
        "Bajaj",
    ],
    "Toys": [
        "Lego",
        "Funskool",
        "Hot Wheels",
        "Hamleys",
        "Mattel",
    ],
}

ORDER_STATUSES = [
    "Delivered",
    "Cancelled",
    "Pending",
    "Returned",
    "Shipped",
]

PAYMENT_METHODS = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "Wallet",
]

PAYMENT_STATUSES = [
    "Success",
    "Failed",
    "Refunded",
    "Pending",
]


# ---------------------------------------------------------
# Utility functions
# ---------------------------------------------------------

def create_directories() -> None:
    """Create all required raw-data directories."""

    directories = [
        CUSTOMERS_DIR,
        PRODUCTS_DIR,
        ORDERS_DIR,
        ORDER_ITEMS_DIR,
        PAYMENTS_DIR,
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


def random_date(start_date: datetime, end_date: datetime) -> datetime:
    """Generate a random datetime between two dates."""

    difference = end_date - start_date
    random_seconds = random.randint(0, int(difference.total_seconds()))

    return start_date + timedelta(seconds=random_seconds)


def generate_customer_id(index: int) -> str:
    return f"CUST{index:06d}"


def generate_product_id(index: int) -> str:
    return f"PROD{index:05d}"


def generate_order_id(index: int) -> str:
    return f"ORD{index:08d}"


def generate_order_item_id(index: int) -> str:
    return f"ITEM{index:09d}"


def generate_payment_id(index: int) -> str:
    return f"PAY{index:08d}"


# ---------------------------------------------------------
# Customer generation
# ---------------------------------------------------------

def generate_customers() -> pd.DataFrame:
    """Generate customer master data."""

    customers = []

    signup_start = datetime(2022, 1, 1)
    signup_end = datetime(2026, 6, 30)

    segment_weights = [0.60, 0.22, 0.13, 0.05]

    for index in range(1, NUM_CUSTOMERS + 1):
        profile = fake.simple_profile()

        signup_date = random_date(
            signup_start,
            signup_end,
        ).date()

        customer = {
            "customer_id": generate_customer_id(index),
            "first_name": profile["name"].split(" ")[0],
            "last_name": profile["name"].split(" ")[-1],
            "email": profile["mail"],
            "phone": fake.phone_number(),
            "city": fake.city(),
            "state": fake.state(),
            "country": "India",
            "signup_date": signup_date,
            "customer_segment": random.choices(
                CUSTOMER_SEGMENTS,
                weights=segment_weights,
                k=1,
            )[0],
        }

        customers.append(customer)

    return pd.DataFrame(customers)


# ---------------------------------------------------------
# Product generation
# ---------------------------------------------------------

def generate_products() -> pd.DataFrame:
    """Generate product master data."""

    products = []

    for index in range(1, NUM_PRODUCTS + 1):
        category = random.choice(PRODUCT_CATEGORIES)
        brand = random.choice(BRANDS_BY_CATEGORY[category])

        price = round(random.uniform(100, 75_000), 2)
        stock_quantity = random.randint(0, 1_500)

        product = {
            "product_id": generate_product_id(index),
            "product_name": f"{brand} {category} Item {index}",
            "category": category,
            "brand": brand,
            "price": price,
            "stock_quantity": stock_quantity,
        }

        products.append(product)

    return pd.DataFrame(products)


# ---------------------------------------------------------
# Order generation
# ---------------------------------------------------------

def generate_orders(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Generate orders linked to existing customers."""

    orders = []

    customer_records = customers_df.to_dict("records")

    order_start = datetime(2024, 1, 1)
    order_end = datetime(2026, 7, 20)

    status_weights = [0.68, 0.08, 0.07, 0.07, 0.10]

    for index in range(1, NUM_ORDERS + 1):
        customer = random.choice(customer_records)

        order_date = random_date(
            order_start,
            order_end,
        )

        order = {
            "order_id": generate_order_id(index),
            "customer_id": customer["customer_id"],
            "order_date": order_date,
            "order_status": random.choices(
                ORDER_STATUSES,
                weights=status_weights,
                k=1,
            )[0],
            "shipping_city": customer["city"],
            "shipping_state": customer["state"],
        }

        orders.append(order)

    return pd.DataFrame(orders)


# ---------------------------------------------------------
# Order item generation
# ---------------------------------------------------------

def generate_order_items(
    orders_df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> pd.DataFrame:
    """Generate order items linked to orders and products."""

    order_items = []

    product_records = products_df.to_dict("records")

    order_item_index = 1

    for order in orders_df.to_dict("records"):
        number_of_items = random.randint(1, 5)

        selected_products = random.sample(
            product_records,
            k=number_of_items,
        )

        for product in selected_products:
            quantity = random.randint(1, 4)

            discount = random.choices(
                [0, 5, 10, 15, 20, 25],
                weights=[35, 20, 18, 12, 10, 5],
                k=1,
            )[0]

            order_item = {
                "order_item_id": generate_order_item_id(
                    order_item_index
                ),
                "order_id": order["order_id"],
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["price"],
                "discount_percentage": discount,
            }

            order_items.append(order_item)
            order_item_index += 1

    return pd.DataFrame(order_items)


# ---------------------------------------------------------
# Payment generation
# ---------------------------------------------------------

def generate_payments(
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame,
) -> pd.DataFrame:
    """Generate one payment record for every order."""

    payments = []

    order_amounts = (
        order_items_df.assign(
            gross_amount=(
                order_items_df["quantity"]
                * order_items_df["unit_price"]
            ),
            discount_amount=(
                order_items_df["quantity"]
                * order_items_df["unit_price"]
                * order_items_df["discount_percentage"]
                / 100
            ),
        )
        .assign(
            net_amount=lambda dataframe: (
                dataframe["gross_amount"]
                - dataframe["discount_amount"]
            )
        )
        .groupby("order_id", as_index=False)["net_amount"]
        .sum()
    )

    orders_with_amounts = orders_df.merge(
        order_amounts,
        on="order_id",
        how="left",
    )

    payment_status_weights = [0.82, 0.08, 0.05, 0.05]

    for index, order in enumerate(
        orders_with_amounts.to_dict("records"),
        start=1,
    ):
        order_date = pd.to_datetime(order["order_date"])

        payment_date = order_date + timedelta(
            minutes=random.randint(1, 120)
        )

        payment_status = random.choices(
            PAYMENT_STATUSES,
            weights=payment_status_weights,
            k=1,
        )[0]

        if order["order_status"] == "Cancelled":
            payment_status = random.choice(
                ["Failed", "Refunded"]
            )

        if order["order_status"] == "Returned":
            payment_status = "Refunded"

        payment = {
            "payment_id": generate_payment_id(index),
            "order_id": order["order_id"],
            "payment_method": random.choice(
                PAYMENT_METHODS
            ),
            "payment_status": payment_status,
            "payment_amount": round(
                float(order["net_amount"]),
                2,
            ),
            "payment_date": payment_date,
        }

        payments.append(payment)

    return pd.DataFrame(payments)


# ---------------------------------------------------------
# Data saving
# ---------------------------------------------------------

def save_dataset(
    dataframe: pd.DataFrame,
    output_path: Path,
) -> None:
    """Save a dataframe as a CSV file."""

    dataframe.to_csv(
        output_path,
        index=False,
    )

    print(
        f"Created: {output_path} "
        f"| Rows: {len(dataframe):,}"
    )


# ---------------------------------------------------------
# Main execution
# ---------------------------------------------------------

def main() -> None:
    print("=" * 70)
    print("RETAIL ANALYTICS DATA GENERATOR")
    print("=" * 70)

    create_directories()

    print("\nGenerating customers...")
    customers_df = generate_customers()

    print("Generating products...")
    products_df = generate_products()

    print("Generating orders...")
    orders_df = generate_orders(customers_df)

    print("Generating order items...")
    order_items_df = generate_order_items(
        orders_df,
        products_df,
    )

    print("Generating payments...")
    payments_df = generate_payments(
        orders_df,
        order_items_df,
    )

    print("\nSaving datasets...")

    save_dataset(
        customers_df,
        CUSTOMERS_DIR / "customers.csv",
    )

    save_dataset(
        products_df,
        PRODUCTS_DIR / "products.csv",
    )

    save_dataset(
        orders_df,
        ORDERS_DIR / "orders.csv",
    )

    save_dataset(
        order_items_df,
        ORDER_ITEMS_DIR / "order_items.csv",
    )

    save_dataset(
        payments_df,
        PAYMENTS_DIR / "payments.csv",
    )

    print("\n" + "=" * 70)
    print("DATA GENERATION COMPLETED SUCCESSFULLY")
    print("=" * 70)

    print("\nGenerated datasets:")
    print(f"Customers:   {len(customers_df):,}")
    print(f"Products:    {len(products_df):,}")
    print(f"Orders:      {len(orders_df):,}")
    print(f"Order Items: {len(order_items_df):,}")
    print(f"Payments:    {len(payments_df):,}")


if __name__ == "__main__":
    main()