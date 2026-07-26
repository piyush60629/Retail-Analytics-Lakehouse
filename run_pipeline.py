from __future__ import annotations

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineStage:
    name: str
    module: str


PIPELINE_STAGES = [
    PipelineStage(
        name="Incremental ingestion and Bronze validation",
        module="src.incremental",
    ),
    PipelineStage(
        name="Record-level CDC",
        module="src.incremental.run_cdc",
    ),
    PipelineStage(
        name="Incremental Silver",
        module="src.incremental_silver",
    ),
    PipelineStage(
        name="SCD Type 2",
        module="src.scd2",
    ),
    PipelineStage(
        name="Fact enrichment",
        module="src.fact_enrichment",
    ),
    PipelineStage(
        name="Gold layer",
        module="src.gold",
    ),
    PipelineStage(
        name="Gold data quality",
        module="src.gold_quality",
    ),
    PipelineStage(
        name="Analytics and KPI layer",
        module="src.analytics",
    ),
]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the complete Retail Analytics "
            "incremental pipeline."
        )
    )

    parser.add_argument(
        "--batch-date",
        required=True,
        help="Batch date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--start-from",
        choices=[
            stage.module
            for stage in PIPELINE_STAGES
        ],
        help=(
            "Optional module from which pipeline "
            "execution should resume."
        ),
    )

    return parser.parse_args()


def get_stages_to_run(
    start_from: str | None,
) -> list[PipelineStage]:
    if start_from is None:
        return PIPELINE_STAGES

    start_index = next(
        index
        for index, stage in enumerate(
            PIPELINE_STAGES
        )
        if stage.module == start_from
    )

    return PIPELINE_STAGES[start_index:]


def run_stage(
    stage: PipelineStage,
    batch_date: str,
) -> None:
    command = [
        sys.executable,
        "-m",
        stage.module,
        "--batch-date",
        batch_date,
    ]

    print("\n" + "=" * 100)
    print(f"STARTING: {stage.name}")
    print(f"COMMAND: {' '.join(command)}")
    print("=" * 100)

    started_at = time.perf_counter()

    subprocess.run(
        command,
        check=True,
    )

    elapsed_seconds = (
        time.perf_counter() - started_at
    )

    print(
        f"\nCOMPLETED: {stage.name} "
        f"in {elapsed_seconds:.2f} seconds"
    )


def main() -> None:
    arguments = parse_arguments()

    stages = get_stages_to_run(
        start_from=arguments.start_from,
    )

    pipeline_started_at = time.perf_counter()

    print("\n" + "=" * 100)
    print("RETAIL ANALYTICS LAKEHOUSE PIPELINE")
    print(f"BATCH DATE: {arguments.batch_date}")
    print("=" * 100)

    try:
        for stage in stages:
            run_stage(
                stage=stage,
                batch_date=arguments.batch_date,
            )

    except subprocess.CalledProcessError as error:
        print("\n" + "!" * 100)
        print("PIPELINE FAILED")
        print(
            f"Failed command: "
            f"{' '.join(error.cmd)}"
        )
        print(
            f"Exit code: {error.returncode}"
        )
        print("Later stages were not executed.")
        print("!" * 100)

        raise SystemExit(
            error.returncode
        ) from error

    total_seconds = (
        time.perf_counter()
        - pipeline_started_at
    )

    print("\n" + "=" * 100)
    print("COMPLETE PIPELINE FINISHED SUCCESSFULLY")
    print(f"BATCH DATE: {arguments.batch_date}")
    print(
        f"TOTAL TIME: {total_seconds:.2f} seconds"
    )
    print("=" * 100)


if __name__ == "__main__":
    main()