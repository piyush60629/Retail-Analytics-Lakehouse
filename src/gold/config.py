from src.common.storage_config import (
    STORAGE_MODE,
    get_storage_path,
)


SCD2_ROOT = get_storage_path(
    "silver_dimensions"
)

ENRICHED_FACT_ROOT = get_storage_path(
    "enriched_facts"
)

GOLD_ROOT = get_storage_path(
    "gold"
)

GOLD_AUDIT_ROOT = get_storage_path(
    "gold_audit"
)


GOLD_TABLES = [
    "dim_customer",
    "dim_product",
    "dim_date",
    "fact_sales",
    "fact_payment",
]