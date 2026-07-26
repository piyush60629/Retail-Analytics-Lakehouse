from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CDC_ROOT = PROJECT_ROOT / "data" / "cdc"
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"

INCREMENTAL_SILVER_AUDIT_ROOT = (
    PROJECT_ROOT
    / "data"
    / "incremental_silver_audit"
)

DATASETS = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]

CDC_METADATA_COLUMNS = [
    "change_type",
    "cdc_batch_date",
    "compared_with_batch_date",
    "cdc_timestamp",
]