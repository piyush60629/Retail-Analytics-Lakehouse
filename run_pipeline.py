from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass

from src.monitoring.run_log import PipelineRunLog, StageOutput


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


class StageFailed(Exception):
    def __init__(self, stage: PipelineStage, command: list[str], returncode: int) -> None:
        super().__init__(f"{stage.name} failed with exit code {returncode}")
        self.stage = stage
        self.command = command
        self.returncode = returncode


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

    parser.add_argument(
        "--no-monitoring",
        action="store_true",
        help="Do not write the run log and batch snapshot under monitoring/.",
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
    run_log: PipelineRunLog | None,
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
    print("=" * 100, flush=True)

    if run_log:
        run_log.stage_started(stage.module)

    started_at = time.perf_counter()
    output = StageOutput()

    # Stream the stage's output to the console as before, and keep the last lines
    # so a failure can be recorded in the run log.
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            sys.stdout.write(line)
            output.add(line)
        returncode = process.wait()

    elapsed_seconds = (
        time.perf_counter() - started_at
    )

    if run_log:
        run_log.stage_finished(stage.module, returncode, elapsed_seconds, output)

    if returncode != 0:
        raise StageFailed(stage, command, returncode)

    print(
        f"\nCOMPLETED: {stage.name} "
        f"in {elapsed_seconds:.2f} seconds"
    )


def main() -> None:
    arguments = parse_arguments()

    stages = get_stages_to_run(
        start_from=arguments.start_from,
    )

    run_log = None
    if not arguments.no_monitoring:
        run_log = PipelineRunLog(
            batch_date=arguments.batch_date,
            stages=[{"name": s.name, "module": s.module} for s in stages],
            start_from=arguments.start_from,
        )
        run_log.save()

    pipeline_started_at = time.perf_counter()

    print("\n" + "=" * 100)
    print("RETAIL ANALYTICS LAKEHOUSE PIPELINE")
    print(f"BATCH DATE: {arguments.batch_date}")
    if run_log:
        print(f"RUN ID:     {run_log.run_id}")
    print("=" * 100)

    completed, exit_code = False, 0
    try:
        for stage in stages:
            run_stage(
                stage=stage,
                batch_date=arguments.batch_date,
                run_log=run_log,
            )
        completed = True

    except StageFailed as error:
        exit_code = error.returncode
        print("\n" + "!" * 100)
        print("PIPELINE FAILED")
        print(
            f"Failed command: "
            f"{' '.join(error.command)}"
        )
        print(
            f"Exit code: {error.returncode}"
        )
        print("Later stages were not executed.")
        print("!" * 100)

    finally:
        total_seconds = (
            time.perf_counter()
            - pipeline_started_at
        )
        if run_log:
            # Anything other than a clean finish (stage failure, Ctrl+C, crash) is recorded as FAILED.
            run_log.finish("SUCCEEDED" if completed else "FAILED", total_seconds)
            from src.monitoring.snapshot import write_snapshot

            snapshot = write_snapshot(run_log.run_id, arguments.batch_date)
            print(f"Run log and batch reports: {snapshot}")

    if exit_code:
        raise SystemExit(exit_code)

    print("\n" + "=" * 100)
    print("COMPLETE PIPELINE FINISHED SUCCESSFULLY")
    print(f"BATCH DATE: {arguments.batch_date}")
    print(
        f"TOTAL TIME: {total_seconds:.2f} seconds"
    )
    print("=" * 100)


if __name__ == "__main__":
    main()
