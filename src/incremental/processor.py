from __future__ import annotations

import traceback
from typing import List

from src.incremental.bronze_adapter import (
    run_validation_and_bronze,
)
from src.incremental.config import (
    INCOMING_ROOT,
    MANIFEST_PATH,
)
from src.incremental.file_manifest import (
    ManifestRecord,
    get_records_by_status,
    reset_processing_files,
    update_file_status,
)
from src.incremental.staging import (
    stage_source_snapshot,
)


def mark_records(
    records: List[ManifestRecord],
    status: str,
    batch_date: str,
    error_message: str | None = None,
) -> None:
    """
    Update the status of multiple manifest records.
    """

    for record in records:
        relative_path = str(
            record["relative_path"]
        )

        update_file_status(
            manifest_path=MANIFEST_PATH,
            relative_path=relative_path,
            status=status,
            batch_date=batch_date,
            error_message=error_message,
        )


def process_pending_files(
    source_root: str,
    batch_date: str,
) -> int:
    """
    Process new or changed source files through validation
    and Bronze ingestion.

    This incremental ingestion utility currently works with
    local filesystem paths because staging and manifest
    management use Python filesystem operations.
    """

    reset_count = reset_processing_files(
        MANIFEST_PATH
    )

    if reset_count > 0:
        print(
            f"Reset {reset_count} interrupted "
            f"PROCESSING record(s) to PENDING."
        )

    pending_records = get_records_by_status(
        manifest_path=MANIFEST_PATH,
        statuses={
            "PENDING",
            "FAILED",
        },
    )

    if not pending_records:
        print(
            "No pending source files were found. "
            "Validation and Bronze ingestion were skipped."
        )
        return 0

    print(
        f"Changed or new files selected: "
        f"{len(pending_records)}"
    )

    for record in pending_records:
        print(
            f"  {record['relative_path']}"
        )

    incoming_directory = stage_source_snapshot(
        source_root=source_root,
        incoming_root=INCOMING_ROOT,
        batch_date=batch_date,
        changed_records=pending_records,
    )

    print(
        f"\nComplete source snapshot staged at: "
        f"{incoming_directory}"
    )

    mark_records(
        records=pending_records,
        status="PROCESSING",
        batch_date=batch_date,
    )

    try:
        run_validation_and_bronze(
            batch_date=batch_date,
        )

        mark_records(
            records=pending_records,
            status="PROCESSED",
            batch_date=batch_date,
        )

        processed_count = len(
            pending_records
        )

        print(
            f"\nSuccessfully processed "
            f"{processed_count} changed file(s)."
        )

        return processed_count

    except Exception as error:
        error_message = (
            f"{type(error).__name__}: {error}"
        )

        mark_records(
            records=pending_records,
            status="FAILED",
            batch_date=batch_date,
            error_message=error_message,
        )

        print(
            "\nIncremental batch failed."
        )
        print(
            error_message
        )

        traceback.print_exc()

        raise