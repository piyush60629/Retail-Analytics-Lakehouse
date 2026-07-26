from __future__ import annotations

from src.common.storage_config import (
    get_storage_path,
)


SILVER_ROOT = get_storage_path(
    "silver"
)

CDC_ROOT = get_storage_path(
    "cdc"
)

SCD2_ROOT = get_storage_path(
    "silver_dimensions"
)

SCD2_AUDIT_ROOT = get_storage_path(
    "scd2_audit"
)


DIMENSION_DATASETS = [
    "customers",
    "products",
]


CDC_METADATA_COLUMNS = [
    "change_type",
    "cdc_batch_date",
    "compared_with_batch_date",
    "cdc_timestamp",
]