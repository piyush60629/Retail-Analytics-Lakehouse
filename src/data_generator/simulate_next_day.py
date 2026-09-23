"""
Simulate one more business day in the source system.

Changes the full snapshots in data/raw/ the way a real OLTP source would:
  - some customers upgrade their segment or move city   (SCD2 updates)
  - new customers sign up                               (CDC inserts)
  - some product prices change                          (SCD2 updates)
  - new orders, order items and payments arrive         (incremental facts)
  - a few bad rows slip in                              (validation / quarantine)

Usage:
    python -m src.data_generator.simulate_next_day --batch-date 2026-07-21
    python run_pipeline.py --batch-date 2026-07-21
"""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

RAW = Path(__file__).resolve().parents[2] / "data" / "raw"
NEXT_SEGMENT = {"Regular": "Silver", "Silver": "Gold", "Gold": "Platinum", "Platinum": "Platinum"}


def path(name: str) -> Path:
    return RAW / name / f"{name}.csv"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    random.seed(args.seed)
    fake = Faker("en_IN")
    Faker.seed(args.seed)
    day = datetime.strptime(args.batch_date, "%Y-%m-%d")

    # dtype=str keeps values byte-for-byte (e.g. "+91..." phones),
    # so unchanged rows are not falsely detected as CDC updates.
    customers = pd.read_csv(path("customers"), dtype=str, keep_default_na=False)
    products = pd.read_csv(path("products"), dtype=str, keep_default_na=False)
    orders = pd.read_csv(path("orders"), dtype=str, keep_default_na=False)
    items = pd.read_csv(path("order_items"), dtype=str, keep_default_na=False)
    payments = pd.read_csv(path("payments"), dtype=str, keep_default_na=False)

    # Customers: segment upgrades, relocations, new sign-ups
    upgrade = customers.sample(150, random_state=args.seed).index
    customers.loc[upgrade, "customer_segment"] = customers.loc[upgrade, "customer_segment"].map(NEXT_SEGMENT)
    moved = customers.sample(60, random_state=args.seed + 1).index
    customers.loc[moved, "city"] = [fake.city() for _ in moved]
    customers.loc[moved, "state"] = [fake.state() for _ in moved]

    next_c = customers["customer_id"].str[4:].astype(int).max() + 1
    new_customers = []
    for i in range(next_c, next_c + 80):
        profile = fake.simple_profile()
        new_customers.append({
            "customer_id": f"CUST{i:06d}",
            "first_name": profile["name"].split(" ")[0],
            "last_name": profile["name"].split(" ")[-1],
            "email": profile["mail"],
            "phone": fake.phone_number(),
            "city": fake.city(),
            "state": fake.state(),
            "country": "India",
            "signup_date": args.batch_date,
            "customer_segment": "Regular",
        })
    customers = pd.concat([customers, pd.DataFrame(new_customers)], ignore_index=True)

    # Products: price changes
    repriced = products.sample(40, random_state=args.seed + 2).index
    products.loc[repriced, "price"] = [
        f"{float(p) * random.uniform(0.85, 1.15):.2f}" for p in products.loc[repriced, "price"]
    ]

    # New orders for the day
    next_o = orders["order_id"].str[3:].astype(int).max() + 1
    next_i = items["order_item_id"].str[4:].astype(int).max() + 1
    next_p = payments["payment_id"].str[3:].astype(int).max() + 1
    cust_ids = customers["customer_id"].tolist()
    prod = products.set_index("product_id")["price"].astype(float)
    new_orders, new_items, new_payments = [], [], []
    for n in range(600):
        oid = f"ORD{next_o + n:08d}"
        ts = day + timedelta(seconds=random.randint(0, 86_399))
        status = random.choices(["Delivered", "Shipped", "Pending", "Cancelled", "Returned"], [0.1, 0.5, 0.3, 0.07, 0.03])[0]
        new_orders.append({"order_id": oid, "customer_id": random.choice(cust_ids),
                           "order_date": ts.strftime("%Y-%m-%d %H:%M:%S"), "order_status": status,
                           "shipping_city": fake.city(), "shipping_state": fake.state()})
        total = 0.0
        for _ in range(random.randint(1, 4)):
            pid = random.choice(prod.index)
            qty = random.randint(1, 5)
            disc = random.choice([0, 5, 10, 15, 20])
            total += qty * prod[pid] * (1 - disc / 100)
            new_items.append({"order_item_id": f"ITEM{next_i + len(new_items):09d}", "order_id": oid,
                              "product_id": pid, "quantity": qty, "unit_price": prod[pid],
                              "discount_percentage": disc})
        pstatus = {"Cancelled": "Failed", "Returned": "Refunded", "Pending": "Pending"}.get(status, "Success")
        new_payments.append({"payment_id": f"PAY{next_p + n:08d}", "order_id": oid,
                             "payment_method": random.choice(["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet"]),
                             "payment_status": pstatus, "payment_amount": round(total, 2),
                             "payment_date": (ts + timedelta(minutes=random.randint(1, 90))).strftime("%Y-%m-%d %H:%M:%S")})

    new_orders = pd.DataFrame(new_orders)
    new_items = pd.DataFrame(new_items)
    new_payments = pd.DataFrame(new_payments)

    # A few bad rows so validation has something to quarantine
    new_items.loc[new_items.sample(8, random_state=1).index, "quantity"] = -2
    new_items.loc[new_items.sample(5, random_state=2).index, "product_id"] = "PROD99999"
    new_orders.loc[new_orders.sample(6, random_state=3).index, "customer_id"] = "CUST999999"
    new_payments.loc[new_payments.sample(5, random_state=4).index, "payment_amount"] = None

    customers.to_csv(path("customers"), index=False)
    products.to_csv(path("products"), index=False)
    pd.concat([orders, new_orders]).to_csv(path("orders"), index=False)
    pd.concat([items, new_items]).to_csv(path("order_items"), index=False)
    pd.concat([payments, new_payments]).to_csv(path("payments"), index=False)

    print(f"Simulated {args.batch_date}: 150 segment upgrades, 60 relocations, 80 new customers, "
          f"40 price changes, {len(new_orders)} new orders, {len(new_items)} new items.")


if __name__ == "__main__":
    main()
