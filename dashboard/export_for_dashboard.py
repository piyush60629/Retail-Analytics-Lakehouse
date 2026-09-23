"""
Copy the PySpark pipeline's results into small CSV files for the dashboard.

Run this after the pipeline has finished for a batch:

    python run_pipeline.py --batch-date 2026-07-21
    python -m dashboard.export_for_dashboard --batch-date 2026-07-21

Spark writes each table as a folder of Parquet/CSV part files. This script
reads those folders with pandas (no Spark or Java needed) and saves one
small CSV per table in dashboard/data/. Commit that folder to GitHub and
the Streamlit app reads it, so the live website never needs Spark.
"""

from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUT = Path(__file__).resolve().parent / "data"

ANALYTICS_TABLES = [
    "daily_sales_summary",
    "product_performance",
    "customer_performance",
    "payment_summary",
    "executive_kpis",
]


def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def read_spark_csv(path: Path) -> pd.DataFrame:
    parts = sorted(glob.glob(str(path / "**" / "part-*.csv"), recursive=True))
    if not parts:
        raise FileNotFoundError(f"No CSV part files in {path}")
    return pd.concat([pd.read_csv(p) for p in parts], ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-date", required=True)
    batch = f"batch_date={parser.parse_args().batch_date}"
    OUT.mkdir(parents=True, exist_ok=True)

    def save(df: pd.DataFrame, name: str) -> None:
        df.to_csv(OUT / f"{name}.csv", index=False)
        print(f"{name:<28} {len(df):>7,} rows")

    # 1. Analytics layer (the KPIs)
    for table in ANALYTICS_TABLES:
        df = read_parquet(DATA / "analytics" / batch / table)
        df = df.drop(columns=[c for c in df.columns if c.endswith("_sk")], errors="ignore")
        save(df, table)

    # 2. Validation results and rule failures
    save(read_spark_csv(DATA / "validation_audit" / batch), "validation_summary")
    save(read_spark_csv(DATA / "data_quality_report" / batch), "validation_rule_failures")

    # 3. Gold data quality checks
    save(read_spark_csv(DATA / "gold_quality_audit" / batch), "gold_quality_results")

    # 4. CDC change counts
    cdc_rows = []
    for folder in sorted((DATA / "cdc" / batch).iterdir()):
        if folder.is_dir():
            counts = read_parquet(folder)["change_type"].value_counts()
            for change_type, n in counts.items():
                cdc_rows.append({"dataset": folder.name, "change_type": change_type, "records": int(n)})
    save(pd.DataFrame(cdc_rows), "cdc_summary")

    # 5. SCD Type 2 history for records that actually changed
    history = []
    for dim, key, cols in (
        ("dim_customer", "customer_id", ["customer_full_name", "city", "state", "customer_segment", "email"]),
        ("dim_product", "product_id", ["product_name", "category", "brand", "price"]),
    ):
        df = read_parquet(DATA / "gold" / batch / dim)
        changed = df.groupby(key)[key].transform("size") > 1
        keep = [key, *[c for c in cols if c in df.columns], "effective_from", "effective_to",
                "is_current", "record_version"]
        part = df.loc[changed, keep].sort_values([key, "record_version"])
        part.insert(0, "dimension", dim)
        part = part.rename(columns={key: "business_key"})
        history.append(part)
    save(pd.concat(history, ignore_index=True), "scd2_history")

    # 6. Row counts through the layers
    layer_rows = []
    validation = read_spark_csv(DATA / "validation_audit" / batch)
    for _, r in validation.iterrows():
        layer_rows.append({"dataset": r["dataset_name"], "layer": "Bronze (raw)", "rows": int(r["total_records"])})
        layer_rows.append({"dataset": r["dataset_name"], "layer": "Validated", "rows": int(r["valid_records"])})
    for table in ["fact_sales", "fact_payment", "dim_customer", "dim_product", "dim_date"]:
        layer_rows.append({"dataset": table, "layer": "Gold",
                           "rows": len(read_parquet(DATA / "gold" / batch / table))})
    save(pd.DataFrame(layer_rows), "layer_counts")

    # 7. Category revenue and customer segments from Gold
    fact = read_parquet(DATA / "gold" / batch / "fact_sales")
    dim_c = read_parquet(DATA / "gold" / batch / "dim_customer")[["customer_sk", "customer_segment"]]
    dim_p = read_parquet(DATA / "gold" / batch / "dim_product")[["product_sk", "category"]]
    fact = fact.merge(dim_c, on="customer_sk", how="left").merge(dim_p, on="product_sk", how="left")
    fact["month"] = pd.to_datetime(fact["order_date"]).dt.to_period("M").dt.to_timestamp()
    for col in ["gross_amount", "discount_amount", "net_amount"]:
        fact[col] = fact[col].astype(float)
    monthly = (fact.groupby(["month", "category", "customer_segment", "order_status"], as_index=False)
               .agg(net_revenue=("net_amount", "sum"), discount=("discount_amount", "sum"),
                    items=("quantity", "sum"), orders=("order_id", "nunique")))
    save(monthly, "monthly_sales_breakdown")

    (OUT / "run_info.json").write_text(json.dumps({"batch_date": batch.split("=")[1]}))
    print(f"\nSaved to {OUT}")


if __name__ == "__main__":
    main()
