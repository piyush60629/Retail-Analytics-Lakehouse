from src.common.storage_config import (
    STORAGE_MODE,
    get_storage_path,
)


GOLD_ROOT = get_storage_path(
    "gold"
)

ANALYTICS_ROOT = get_storage_path(
    "analytics"
)

ANALYTICS_AUDIT_ROOT = get_storage_path(
    "analytics_audit"
)


ANALYTICS_TABLES = [
    "daily_sales_summary",
    "product_performance",
    "customer_performance",
    "payment_summary",
    "executive_kpis",
]