from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Union


PathLike = Union[str, Path]

ManifestRecord = Dict[str, object]
ManifestData = Dict[str, ManifestRecord]


CLOUD_PATH_PREFIXES = (
    "abfss://",
    "wasbs://",
    "dbfs:/",
    "s3://",
    "gs://",
)


def utc_timestamp() -> str:
    """
    Return the current UTC timestamp in ISO format.
    """

    return datetime.now(
        timezone.utc
    ).isoformat()


def to_local_path(
    path_value: PathLike,
    path_description: str,
) -> Path:
    """
    Convert a string or Path value into a local Path.

    This incremental file-manifest implementation uses Python
    filesystem operations and therefore supports local paths only.

    ADLS ingestion will later be handled through Azure Data Factory
    or a cloud-native control table.
    """

    path_string = str(path_value).strip()

    if not path_string:
        raise ValueError(
            f"{path_description} cannot be empty."
        )

    if path_string.startswith(
        CLOUD_PATH_PREFIXES
    ):
        raise ValueError(
            f"{path_description} uses a cloud path that "
            f"is not supported by the local file-manifest "
            f"scanner: {path_string}. "
            f"Use Azure Data Factory for cloud file ingestion."
        )

    return Path(path_string)


def calculate_file_checksum(
    file_path: PathLike,
    chunk_size: int = 1024 * 1024,
) -> str:
    """
    Calculate the SHA-256 checksum of a local file.

    The file is read in chunks so large files are not loaded
    completely into memory.
    """

    local_file_path = to_local_path(
        file_path,
        "Source file path",
    )

    if not local_file_path.exists():
        raise FileNotFoundError(
            f"Source file not found: "
            f"{local_file_path}"
        )

    if not local_file_path.is_file():
        raise ValueError(
            f"Checksum path is not a file: "
            f"{local_file_path}"
        )

    sha256 = hashlib.sha256()

    with local_file_path.open(
        "rb"
    ) as source_file:
        while True:
            chunk = source_file.read(
                chunk_size
            )

            if not chunk:
                break

            sha256.update(chunk)

    return sha256.hexdigest()


def load_manifest(
    manifest_path: PathLike,
) -> ManifestData:
    """
    Read the existing file manifest.

    Return an empty manifest when the file does not exist.
    """

    local_manifest_path = to_local_path(
        manifest_path,
        "Manifest path",
    )

    if not local_manifest_path.exists():
        return {}

    if not local_manifest_path.is_file():
        raise ValueError(
            f"Manifest path is not a file: "
            f"{local_manifest_path}"
        )

    try:
        with local_manifest_path.open(
            "r",
            encoding="utf-8",
        ) as manifest_file:
            content = json.load(
                manifest_file
            )

        if not isinstance(content, dict):
            raise ValueError(
                "Manifest content must be a JSON object."
            )

        return content

    except json.JSONDecodeError as error:
        raise ValueError(
            f"Invalid manifest JSON: "
            f"{local_manifest_path}"
        ) from error


def save_manifest(
    manifest: ManifestData,
    manifest_path: PathLike,
) -> None:
    """
    Save the manifest atomically.

    A temporary file is created first and then replaces the
    existing manifest.
    """

    local_manifest_path = to_local_path(
        manifest_path,
        "Manifest path",
    )

    local_manifest_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = (
        local_manifest_path.with_suffix(
            local_manifest_path.suffix
            + ".tmp"
        )
    )

    try:
        with temporary_path.open(
            "w",
            encoding="utf-8",
        ) as manifest_file:
            json.dump(
                manifest,
                manifest_file,
                indent=2,
                sort_keys=True,
            )

        temporary_path.replace(
            local_manifest_path
        )

    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()

        raise


def create_file_record(
    file_path: PathLike,
    source_root: PathLike,
    checksum: str,
    status: str,
) -> ManifestRecord:
    """
    Create a manifest record for a local source file.
    """

    local_file_path = to_local_path(
        file_path,
        "Source file path",
    )

    local_source_root = to_local_path(
        source_root,
        "Source root",
    ).resolve()

    resolved_file_path = (
        local_file_path.resolve()
    )

    try:
        relative_path = (
            resolved_file_path
            .relative_to(local_source_root)
            .as_posix()
        )

    except ValueError as error:
        raise ValueError(
            f"Source file is outside the source root. "
            f"File: {resolved_file_path}; "
            f"Root: {local_source_root}"
        ) from error

    file_stats = resolved_file_path.stat()

    return {
        "relative_path": relative_path,
        "file_name": resolved_file_path.name,
        "checksum": checksum,
        "file_size_bytes": file_stats.st_size,
        "source_modified_at": (
            datetime.fromtimestamp(
                file_stats.st_mtime,
                tz=timezone.utc,
            ).isoformat()
        ),
        "status": status,
        "detected_at": utc_timestamp(),
        "processing_started_at": None,
        "processed_at": None,
        "failed_at": None,
        "batch_date": None,
        "error_message": None,
    }


def discover_source_files(
    source_root: PathLike,
    file_pattern: str = "*.csv",
) -> List[Path]:
    """
    Discover local source files recursively.
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

    return sorted(
        file_path
        for file_path in local_source_root.rglob(
            file_pattern
        )
        if file_path.is_file()
    )


def scan_incremental_files(
    source_root: PathLike,
    manifest_path: PathLike,
    file_pattern: str = "*.csv",
) -> Dict[str, List[ManifestRecord]]:
    """
    Compare source files with the existing manifest.

    Classifications:

    NEW:
        File path has never been recorded.

    CHANGED:
        File exists in the manifest but its checksum changed.

    UNCHANGED:
        File path and checksum match the manifest.
    """

    local_source_root = to_local_path(
        source_root,
        "Source root",
    ).resolve()

    manifest = load_manifest(
        manifest_path
    )

    discovered_files = discover_source_files(
        source_root=local_source_root,
        file_pattern=file_pattern,
    )

    results: Dict[
        str,
        List[ManifestRecord],
    ] = {
        "new": [],
        "changed": [],
        "unchanged": [],
    }

    for file_path in discovered_files:
        checksum = calculate_file_checksum(
            file_path
        )

        relative_path = (
            file_path.resolve()
            .relative_to(local_source_root)
            .as_posix()
        )

        existing_record = manifest.get(
            relative_path
        )

        if existing_record is None:
            record = create_file_record(
                file_path=file_path,
                source_root=local_source_root,
                checksum=checksum,
                status="PENDING",
            )

            manifest[relative_path] = record
            results["new"].append(record)
            continue

        previous_checksum = (
            existing_record.get(
                "checksum"
            )
        )

        if previous_checksum != checksum:
            record = create_file_record(
                file_path=file_path,
                source_root=local_source_root,
                checksum=checksum,
                status="PENDING",
            )

            record["previous_checksum"] = (
                previous_checksum
            )

            manifest[relative_path] = record
            results["changed"].append(record)
            continue

        results["unchanged"].append(
            existing_record
        )

    save_manifest(
        manifest=manifest,
        manifest_path=manifest_path,
    )

    return results


def get_pending_files(
    manifest_path: PathLike,
) -> List[ManifestRecord]:
    """
    Return files waiting to be processed.
    """

    manifest = load_manifest(
        manifest_path
    )

    return [
        record
        for record in manifest.values()
        if record.get("status") == "PENDING"
    ]


def update_file_status(
    manifest_path: PathLike,
    relative_path: str,
    status: str,
    batch_date: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """
    Update the processing status of one manifest record.
    """

    allowed_statuses = {
        "PENDING",
        "PROCESSING",
        "PROCESSED",
        "FAILED",
    }

    if status not in allowed_statuses:
        raise ValueError(
            f"Unsupported manifest status: {status}"
        )

    manifest = load_manifest(
        manifest_path
    )

    if relative_path not in manifest:
        raise KeyError(
            f"Manifest record not found: "
            f"{relative_path}"
        )

    record = manifest[relative_path]

    record["status"] = status
    record["batch_date"] = batch_date
    record["error_message"] = error_message

    if status == "PROCESSING":
        record["processing_started_at"] = (
            utc_timestamp()
        )

    elif status == "PROCESSED":
        record["processed_at"] = (
            utc_timestamp()
        )
        record["failed_at"] = None
        record["error_message"] = None

    elif status == "FAILED":
        record["failed_at"] = (
            utc_timestamp()
        )

    elif status == "PENDING":
        record["processing_started_at"] = None

    manifest[relative_path] = record

    save_manifest(
        manifest=manifest,
        manifest_path=manifest_path,
    )


def get_records_by_status(
    manifest_path: PathLike,
    statuses: set[str],
) -> List[ManifestRecord]:
    """
    Return manifest records matching the supplied statuses.
    """

    manifest = load_manifest(
        manifest_path
    )

    return [
        record
        for record in manifest.values()
        if str(
            record.get("status")
        ) in statuses
    ]


def reset_processing_files(
    manifest_path: PathLike,
) -> int:
    """
    Reset files left in PROCESSING after an interrupted run.
    """

    manifest = load_manifest(
        manifest_path
    )

    reset_count = 0

    for relative_path, record in manifest.items():
        if (
            record.get("status")
            != "PROCESSING"
        ):
            continue

        record["status"] = "PENDING"
        record["processing_started_at"] = None
        record["error_message"] = (
            "Reset after interrupted "
            "processing run."
        )

        manifest[relative_path] = record
        reset_count += 1

    if reset_count > 0:
        save_manifest(
            manifest=manifest,
            manifest_path=manifest_path,
        )

    return reset_count