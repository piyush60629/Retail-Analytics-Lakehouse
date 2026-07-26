from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import List, Union

from src.incremental.file_manifest import (
    ManifestRecord,
    utc_timestamp,
)


PathLike = Union[str, Path]


DATASET_SOURCE_PATHS = {
    "customers": Path(
        "customers/customers.csv"
    ),
    "products": Path(
        "products/products.csv"
    ),
    "orders": Path(
        "orders/orders.csv"
    ),
    "order_items": Path(
        "order_items/order_items.csv"
    ),
    "payments": Path(
        "payments/payments.csv"
    ),
}


CLOUD_PATH_PREFIXES = (
    "abfss://",
    "wasbs://",
    "dbfs:/",
    "s3://",
    "gs://",
)


def to_local_path(
    path_value: PathLike,
    path_description: str,
) -> Path:
    """
    Convert a string or Path into a local Path.

    This staging module uses Python filesystem operations such
    as shutil.copy2 and therefore currently supports local
    filesystem paths only.
    """

    path_string = str(
        path_value
    ).strip()

    if not path_string:
        raise ValueError(
            f"{path_description} cannot be empty."
        )

    if path_string.startswith(
        CLOUD_PATH_PREFIXES
    ):
        raise ValueError(
            f"{path_description} uses a cloud path that "
            f"is not supported by the local staging module: "
            f"{path_string}. "
            f"Azure Data Factory will handle cloud staging."
        )

    return Path(
        path_string
    )


def create_incoming_batch_directory(
    incoming_root: PathLike,
    batch_date: str,
) -> Path:
    """
    Create a clean local incoming directory for one batch.
    """

    local_incoming_root = to_local_path(
        incoming_root,
        "Incoming root",
    )

    batch_directory = (
        local_incoming_root
        / f"batch_date={batch_date}"
    )

    if batch_directory.exists():
        shutil.rmtree(
            batch_directory
        )

    batch_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    return batch_directory


def stage_source_snapshot(
    source_root: PathLike,
    incoming_root: PathLike,
    batch_date: str,
    changed_records: List[ManifestRecord],
) -> Path:
    """
    Copy the complete current source snapshot into incoming.

    Even when only one source file changes, all datasets are
    staged because validation performs cross-dataset checks.

    This utility currently supports local filesystem paths.
    """

    local_source_root = to_local_path(
        source_root,
        "Source root",
    ).resolve()

    if not local_source_root.exists():
        raise FileNotFoundError(
            f"Source directory not found: "
            f"{local_source_root}"
        )

    if not local_source_root.is_dir():
        raise NotADirectoryError(
            f"Source root is not a directory: "
            f"{local_source_root}"
        )

    batch_directory = (
        create_incoming_batch_directory(
            incoming_root=incoming_root,
            batch_date=batch_date,
        )
    )

    staged_files = []

    try:
        for dataset_name, relative_path in (
            DATASET_SOURCE_PATHS.items()
        ):
            source_file = (
                local_source_root
                / relative_path
            )

            if not source_file.exists():
                raise FileNotFoundError(
                    f"Required source file not found: "
                    f"{source_file}"
                )

            if not source_file.is_file():
                raise ValueError(
                    f"Source path is not a file: "
                    f"{source_file}"
                )

            destination_file = (
                batch_directory
                / f"{dataset_name}_{batch_date}.csv"
            )

            shutil.copy2(
                source_file,
                destination_file,
            )

            staged_files.append(
                {
                    "dataset_name": dataset_name,
                    "source_path": (
                        relative_path.as_posix()
                    ),
                    "destination_file": (
                        destination_file.name
                    ),
                    "file_size_bytes": (
                        destination_file
                        .stat()
                        .st_size
                    ),
                }
            )

        changed_paths = [
            str(
                record["relative_path"]
            )
            for record in changed_records
        ]

        metadata = {
            "batch_date": batch_date,
            "created_at": utc_timestamp(),
            "triggered_by_files": changed_paths,
            "staged_file_count": len(
                staged_files
            ),
            "files": staged_files,
        }

        metadata_path = (
            batch_directory
            / "_batch_manifest.json"
        )

        temporary_metadata_path = (
            batch_directory
            / "_batch_manifest.json.tmp"
        )

        with temporary_metadata_path.open(
            "w",
            encoding="utf-8",
        ) as metadata_file:
            json.dump(
                metadata,
                metadata_file,
                indent=2,
            )

        temporary_metadata_path.replace(
            metadata_path
        )

        return batch_directory

    except Exception:
        if batch_directory.exists():
            shutil.rmtree(
                batch_directory
            )

        raise