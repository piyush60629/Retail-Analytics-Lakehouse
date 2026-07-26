from __future__ import annotations

import sys
from pathlib import Path

from src.common.storage_config import (
    LOCAL_DATA_ROOT,
    join_storage_path,
)


# Project-level root retained for backward compatibility
# with bronze_adapter.py and other local utilities.
PROJECT_ROOT = Path(__file__).resolve().parents[2]


# This incremental file-scanning utility remains local because
# it uses pathlib, JSON manifests, shutil and subprocess commands.
DEFAULT_SOURCE_ROOT = join_storage_path(
    LOCAL_DATA_ROOT,
    "raw",
)

CONTROL_ROOT = join_storage_path(
    LOCAL_DATA_ROOT,
    "control",
)

MANIFEST_PATH = join_storage_path(
    CONTROL_ROOT,
    "file_manifest.json",
)

INCOMING_ROOT = join_storage_path(
    LOCAL_DATA_ROOT,
    "processed",
)

BRONZE_ROOT = join_storage_path(
    LOCAL_DATA_ROOT,
    "bronze",
)

CDC_ROOT = join_storage_path(
    LOCAL_DATA_ROOT,
    "cdc",
)


def build_validation_command(
    batch_date: str,
) -> list[str]:
    """
    Build the Bronze validation command.
    """

    return [
        sys.executable,
        "-m",
        "src.validation.bronze_validator",
        "--batch-date",
        batch_date,
    ]


def build_bronze_command(
    batch_date: str,
) -> list[str]:
    """
    Build the Bronze ingestion command for one batch.
    """

    return [
        sys.executable,
        "-m",
        "src.ingestion.bronze_ingestion",
        "--batch-date",
        batch_date,
    ]