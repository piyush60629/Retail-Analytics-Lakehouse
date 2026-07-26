from __future__ import annotations

import os
from pathlib import Path
from typing import Union

from dotenv import load_dotenv


# Load values from the .env file.
load_dotenv()


# Project root:
# RetailAnalyticsLakehouse/
PROJECT_ROOT = Path(__file__).resolve().parents[2]


PathLike = Union[str, Path]


def join_storage_path(
    base_path: PathLike,
    *parts: PathLike,
) -> str:
    """
    Join local and cloud storage paths safely.

    Supports:
    - Windows paths
    - Linux paths
    - ADLS Gen2 ABFSS paths
    - DBFS paths
    - S3 paths
    - Google Cloud Storage paths

    Local example:
        C:/project/data/gold/batch_date=2026-07-26

    ADLS example:
        abfss://gold@account.dfs.core.windows.net/
        batch_date=2026-07-26
    """

    # Convert Path objects such as WindowsPath
    # into strings before using string methods.
    base_path_string = str(base_path)

    cleaned_parts = [
        str(part).strip("/\\")
        for part in parts
        if (
            part is not None
            and str(part).strip()
        )
    ]

    # Handle cloud-style paths.
    if base_path_string.startswith(
        (
            "abfss://",
            "wasbs://",
            "dbfs:/",
            "s3://",
            "gs://",
        )
    ):
        cleaned_base = base_path_string.rstrip("/")

        if not cleaned_parts:
            return cleaned_base

        return (
            cleaned_base
            + "/"
            + "/".join(cleaned_parts)
        )

    # Handle Windows or local filesystem paths.
    local_path = Path(base_path_string)

    for part in cleaned_parts:
        local_path = (
            local_path
            / part
        )

    return str(local_path)


STORAGE_MODE = os.getenv(
    "STORAGE_MODE",
    "local",
).strip().lower()


if STORAGE_MODE not in {
    "local",
    "adls",
}:
    raise ValueError(
        "STORAGE_MODE must be 'local' or 'adls'."
    )


LOCAL_DATA_ROOT = os.getenv(
    "LOCAL_DATA_ROOT",
    "data",
).strip()


local_data_path = Path(
    LOCAL_DATA_ROOT
)

if not local_data_path.is_absolute():
    local_data_path = (
        PROJECT_ROOT
        / local_data_path
    )

LOCAL_DATA_ROOT = str(
    local_data_path.resolve()
)


ADLS_STORAGE_ACCOUNT = os.getenv(
    "ADLS_STORAGE_ACCOUNT",
    "",
).strip()


ADLS_LANDING_CONTAINER = os.getenv(
    "ADLS_LANDING_CONTAINER",
    "landing",
).strip()

ADLS_BRONZE_CONTAINER = os.getenv(
    "ADLS_BRONZE_CONTAINER",
    "bronze",
).strip()

ADLS_SILVER_CONTAINER = os.getenv(
    "ADLS_SILVER_CONTAINER",
    "silver",
).strip()

ADLS_GOLD_CONTAINER = os.getenv(
    "ADLS_GOLD_CONTAINER",
    "gold",
).strip()

ADLS_ANALYTICS_CONTAINER = os.getenv(
    "ADLS_ANALYTICS_CONTAINER",
    "analytics",
).strip()

ADLS_AUDIT_CONTAINER = os.getenv(
    "ADLS_AUDIT_CONTAINER",
    "audit",
).strip()


def build_adls_container_path(
    container_name: str,
) -> str:
    """
    Build an ADLS Gen2 ABFSS container path.
    """

    if not container_name:
        raise ValueError(
            "ADLS container name cannot be empty."
        )

    if not ADLS_STORAGE_ACCOUNT:
        raise ValueError(
            "ADLS_STORAGE_ACCOUNT is required "
            "when STORAGE_MODE=adls."
        )

    return (
        f"abfss://{container_name}"
        f"@{ADLS_STORAGE_ACCOUNT}"
        ".dfs.core.windows.net"
    )


def get_local_storage_layers() -> dict[str, str]:
    """
    Return all local storage layer paths.
    """

    return {
        "raw": join_storage_path(
            LOCAL_DATA_ROOT,
            "raw",
        ),
        "incoming": join_storage_path(
            LOCAL_DATA_ROOT,
            "incoming",
        ),
        "processed": join_storage_path(
            LOCAL_DATA_ROOT,
            "processed",
        ),
        "bronze": join_storage_path(
            LOCAL_DATA_ROOT,
            "bronze",
        ),
        "silver": join_storage_path(
            LOCAL_DATA_ROOT,
            "silver",
        ),
        "cdc": join_storage_path(
            LOCAL_DATA_ROOT,
            "cdc",
        ),
        "silver_dimensions": join_storage_path(
            LOCAL_DATA_ROOT,
            "silver_dimensions",
        ),
        "enriched_facts": join_storage_path(
            LOCAL_DATA_ROOT,
            "enriched_facts",
        ),
        "gold": join_storage_path(
            LOCAL_DATA_ROOT,
            "gold",
        ),
        "analytics": join_storage_path(
            LOCAL_DATA_ROOT,
            "analytics",
        ),

        "scd2_audit": join_storage_path(
            LOCAL_DATA_ROOT,
            "scd2_audit",
        ),

        "validation_audit": join_storage_path(
            LOCAL_DATA_ROOT,
            "validation_audit",
        ),
        "gold_audit": join_storage_path(
            LOCAL_DATA_ROOT,
            "gold_audit",
        ),
        "gold_quality_audit": join_storage_path(
            LOCAL_DATA_ROOT,
            "gold_quality_audit",
        ),
        "fact_enrichment_audit": (
            join_storage_path(
                LOCAL_DATA_ROOT,
                "fact_enrichment_audit",
            )
        ),
        "control": join_storage_path(
            LOCAL_DATA_ROOT,
            "control",
        ),
        "analytics_audit": join_storage_path(
            LOCAL_DATA_ROOT,
            "analytics_audit",
        ),
    }


def get_adls_storage_layers() -> dict[str, str]:
    """
    Return all ADLS Gen2 storage layer paths.
    """

    landing_root = build_adls_container_path(
        ADLS_LANDING_CONTAINER
    )

    bronze_root = build_adls_container_path(
        ADLS_BRONZE_CONTAINER
    )

    silver_root = build_adls_container_path(
        ADLS_SILVER_CONTAINER
    )

    gold_root = build_adls_container_path(
        ADLS_GOLD_CONTAINER
    )

    analytics_root = build_adls_container_path(
        ADLS_ANALYTICS_CONTAINER
    )

    audit_root = build_adls_container_path(
        ADLS_AUDIT_CONTAINER
    )
    

    return {
        "raw": join_storage_path(
            landing_root,
            "raw",
        ),
        "incoming": join_storage_path(
            landing_root,
            "incoming",
        ),
        "processed": join_storage_path(
            landing_root,
            "processed",
        ),
        "bronze": bronze_root,
        "silver": join_storage_path(
            silver_root,
            "standardized",
        ),
        "cdc": join_storage_path(
            silver_root,
            "cdc",
        ),
        "silver_dimensions": join_storage_path(
            silver_root,
            "dimensions",
        ),
        "enriched_facts": join_storage_path(
            silver_root,
            "enriched_facts",
        ),
        "gold": gold_root,
        "analytics": analytics_root,

        "scd2_audit": join_storage_path(
            audit_root,
            "scd2",
        ),

        "validation_audit": join_storage_path(
            audit_root,
            "validation",
        ),
        "gold_audit": join_storage_path(
            audit_root,
            "gold",
        ),
        "gold_quality_audit": join_storage_path(
            audit_root,
            "gold_quality",
        ),
        "fact_enrichment_audit": (
            join_storage_path(
                audit_root,
                "fact_enrichment",
            )
        ),
        "control": join_storage_path(
            audit_root,
            "control",
        ),
        "analytics_audit": join_storage_path(
            audit_root,
            "analytics",
        ),
    }


def get_storage_path(
    layer_name: str,
    *parts: PathLike,
) -> str:
    """
    Return a local or ADLS path based on STORAGE_MODE.

    Examples:

        get_storage_path("silver")

        get_storage_path(
            "silver",
            "batch_date=2026-07-26",
            "orders",
        )
    """

    if STORAGE_MODE == "local":
        storage_layers = (
            get_local_storage_layers()
        )
    else:
        storage_layers = (
            get_adls_storage_layers()
        )

    if layer_name not in storage_layers:
        available_layers = ", ".join(
            sorted(storage_layers.keys())
        )

        raise KeyError(
            f"Unknown storage layer: {layer_name}. "
            f"Available layers: {available_layers}"
        )

    return join_storage_path(
        storage_layers[layer_name],
        *parts,
    )