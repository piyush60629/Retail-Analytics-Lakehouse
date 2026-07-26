from __future__ import annotations

from src.common.storage_config import (
    get_storage_path,
)


SILVER_ROOT = get_storage_path(
    "silver"
)

SCD2_ROOT = get_storage_path(
    "silver_dimensions"
)

ENRICHED_FACT_ROOT = get_storage_path(
    "enriched_facts"
)

FACT_ENRICHMENT_AUDIT_ROOT = get_storage_path(
    "fact_enrichment_audit"
)

FACT_DATASETS = [
    "orders",
    "order_items",
    "payments",
]