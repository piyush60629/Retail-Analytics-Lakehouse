from src.common.storage_config import (
    STORAGE_MODE,
    get_storage_path,
)

GOLD_ROOT = get_storage_path(
    "gold"
)

ENRICHED_FACT_ROOT = get_storage_path(
    "enriched_facts"
)

QUALITY_AUDIT_ROOT = get_storage_path(
    "gold_quality_audit"
)


GOLD_TABLES = [
    "dim_customer",
    "dim_product",
    "dim_date",
    "fact_sales",
    "fact_payment",
]