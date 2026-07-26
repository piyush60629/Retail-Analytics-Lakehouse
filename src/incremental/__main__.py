import argparse
from pathlib import Path

from src.incremental.config import (
    DEFAULT_SOURCE_ROOT,
)
from src.incremental.runner import (
    run_incremental_pipeline,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Detect and process new or changed "
            "source files incrementally."
        )
    )

    parser.add_argument(
        "--source-root",
        type=Path,
        default=DEFAULT_SOURCE_ROOT,
        help=(
            "Directory containing source CSV files. "
            "Defaults to data/source."
        ),
    )

    parser.add_argument(
        "--file-pattern",
        default="*.csv",
        help="File pattern to scan recursively.",
    )

    parser.add_argument(
        "--batch-date",
        help="Processing date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--scan-only",
        action="store_true",
        help=(
            "Detect files without running "
            "Bronze ingestion."
        ),
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    run_incremental_pipeline(
        source_root=arguments.source_root,
        file_pattern=arguments.file_pattern,
        batch_date=arguments.batch_date,
        scan_only=arguments.scan_only,
    )


if __name__ == "__main__":
    main()