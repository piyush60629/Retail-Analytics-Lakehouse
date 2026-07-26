from __future__ import annotations

import subprocess
from typing import Sequence

from src.incremental.config import (
    PROJECT_ROOT,
    build_bronze_command,
    build_validation_command,
)


class PipelineExecutionError(RuntimeError):
    """
    Raised when a child pipeline command fails.
    """


def run_command(
    command: Sequence[str],
    stage_name: str,
) -> None:
    """
    Run one existing project stage as a child process.

    The command is executed from the project root so Python
    module imports and relative project paths resolve correctly.
    """

    printable_command = " ".join(
        str(value)
        for value in command
    )

    print(f"\nRunning {stage_name}:")
    print(printable_command)
    print()

    completed_process = subprocess.run(
        [str(value) for value in command],
        cwd=str(PROJECT_ROOT),
        text=True,
        capture_output=True,
        check=False,
    )

    if completed_process.stdout:
        print(
            completed_process.stdout,
            end=""
            if completed_process.stdout.endswith("\n")
            else "\n",
        )

    if completed_process.stderr:
        print(
            completed_process.stderr,
            end=""
            if completed_process.stderr.endswith("\n")
            else "\n",
        )

    if completed_process.returncode != 0:
        raise PipelineExecutionError(
            f"{stage_name} failed with exit code "
            f"{completed_process.returncode}."
        )


def run_validation_and_bronze(
    batch_date: str,
) -> None:
    """
    Run Bronze ingestion followed by Bronze validation.

    The validator reads Bronze Parquet datasets, so Bronze
    ingestion must complete before validation starts.
    """

    bronze_command = build_bronze_command(
        batch_date=batch_date,
    )

    run_command(
        command=bronze_command,
        stage_name="Bronze ingestion",
    )

    validation_command = build_validation_command(
        batch_date=batch_date,
    )

    run_command(
        command=validation_command,
        stage_name="Bronze validation",
    )