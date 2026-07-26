from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
SILVER_AUDIT_ROOT = PROJECT_ROOT / "data" / "silver_audit"


DATASETS = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]