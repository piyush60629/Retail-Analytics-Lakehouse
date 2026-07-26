from __future__ import annotations

from typing import Dict, List

from src.incremental.config import (
    DEFAULT_SOURCE_ROOT,
    MANIFEST_PATH,
)
from src.incremental.file_manifest import (
    ManifestRecord,
    get_records_by_status,
    scan_incremental_files,
)
from src.incremental.processor import (
    process_pending_files,
)


def print_file_group(
    title: str,
    records: List[ManifestRecord],
) -> None:
    """
    Print files grouped by incremental scan status.
    """

    print(f"\n{title}: {len(records)}")

    for record in records:
        print(
            f"  {record['relative_path']} | "
            f"{record['file_size_bytes']} bytes"
        )


def scan_source_files(
    source_root: str,
    file_pattern: str,
) -> Dict[str, List[ManifestRecord]]:
    """
    Scan the source location and identify new, changed,
    and unchanged files.

    The source root is represented as a string so the same
    runner can receive either a local filesystem path or a
    cloud storage URI.
    """

    results = scan_incremental_files(
        source_root=source_root,
        manifest_path=MANIFEST_PATH,
        file_pattern=file_pattern,
    )

    print_file_group(
        "NEW FILES",
        results["new"],
    )

    print_file_group(
        "CHANGED FILES",
        results["changed"],
    )

    print_file_group(
        "UNCHANGED FILES",
        results["unchanged"],
    )

    return results


def run_incremental_pipeline(
    source_root: str = DEFAULT_SOURCE_ROOT,
    file_pattern: str = "*.csv",
    batch_date: str | None = None,
    scan_only: bool = False,
) -> None:
    """
    Run the file-level incremental ingestion pipeline.

    Steps:
    1. Scan the source location.
    2. Compare files with the manifest.
    3. Identify new or changed files.
    4. Process pending files.
    5. Trigger downstream validation and Bronze ingestion
       through the existing processor implementation.
    """

    print("=" * 100)
    print("INCREMENTAL INGESTION PIPELINE")
    print("=" * 100)
    print(f"Source root:  {source_root}")
    print(f"Manifest:     {MANIFEST_PATH}")
    print(f"File pattern: {file_pattern}")
    print(
        f"Batch date:   "
        f"{batch_date or 'Not provided'}"
    )
    print("=" * 100)

    scan_source_files(
        source_root=source_root,
        file_pattern=file_pattern,
    )

    pending_files = get_records_by_status(
        manifest_path=MANIFEST_PATH,
        statuses={
            "PENDING",
            "FAILED",
        },
    )

    print(
        f"\nFiles waiting for processing: "
        f"{len(pending_files)}"
    )

    if scan_only:
        print(
            "Scan-only mode enabled. "
            "Bronze ingestion was not started."
        )
        return

    if batch_date is None:
        raise ValueError(
            "--batch-date is required unless "
            "--scan-only is supplied."
        )

    processed_count = process_pending_files(
        source_root=source_root,
        batch_date=batch_date,
    )

    remaining_files = get_records_by_status(
        manifest_path=MANIFEST_PATH,
        statuses={
            "PENDING",
            "FAILED",
        },
    )

    print("\n" + "=" * 100)
    print("INCREMENTAL PIPELINE SUMMARY")
    print("=" * 100)
    print(
        f"Files processed: "
        f"{processed_count}"
    )
    print(
        f"Files remaining: "
        f"{len(remaining_files)}"
    )
    print("=" * 100)