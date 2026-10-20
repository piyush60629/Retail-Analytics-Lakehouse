"""Rebuild the lakehouse from scratch and run realistic failure scenarios.

Each scenario starts from the same healthy state (batches 2026-07-20 and 2026-07-21 loaded),
simulates the next day's source files for 2026-07-22, breaks them the way real source systems
do, and runs the real pipeline. Every run writes its run log and batch reports under
monitoring/runs/<run_id>/, which is what the Pipeline Incident Agent reads.

    python scripts/run_failure_scenarios.py --yes                 # all scenarios (about 25 minutes)
    python scripts/run_failure_scenarios.py --yes --only duplicate_orders

WARNING: this deletes and regenerates data/ and monitoring/ (both are generated outputs).
Which run belongs to which scenario is written to scenarios/results.json, outside monitoring/,
so the run logs themselves never reveal the cause.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RAW = DATA / "raw"
MONITORING = ROOT / "monitoring"
BASELINE = ROOT / ".scenario_baseline"
RESULTS = ROOT / "scenarios" / "results.json"
SCENARIO_BATCH = "2026-07-22"


def run(*args: str, check: bool = True) -> int:
    print("$", " ".join(args), flush=True)
    env = {**os.environ, "PIPELINE_TRIGGERED_BY": "schedule"}
    return subprocess.run([sys.executable, *args], cwd=ROOT, check=check, env=env).returncode


# --------------------------------------------------------------------------- #
# Scenarios: each breaks the source files for SCENARIO_BATCH in one realistic way
# --------------------------------------------------------------------------- #


def new_column_in_products() -> None:
    """The product master export adds a 'currency' column in the middle of the file."""
    df = pd.read_csv(RAW / "products/products.csv", dtype=str)
    df.insert(4, "currency", "INR")
    df.to_csv(RAW / "products/products.csv", index=False)


def text_in_price_column() -> None:
    """60 products arrive with price 'N/A' (the source system had no price yet)."""
    df = pd.read_csv(RAW / "products/products.csv", dtype=str)
    df.loc[df.index[:60], "price"] = "N/A"
    df.to_csv(RAW / "products/products.csv", index=False)


def duplicate_orders() -> None:
    """The order export retried a page and wrote 800 orders twice."""
    df = pd.read_csv(RAW / "orders/orders.csv", dtype=str)
    df = pd.concat([df, df.sample(800, random_state=1)])
    df.to_csv(RAW / "orders/orders.csv", index=False)


def truncated_payments() -> None:
    """The payments export was interrupted: the file stops at 40% of its size, mid-row."""
    path = RAW / "payments/payments.csv"
    content = path.read_bytes()
    path.write_bytes(content[: int(len(content) * 0.4)])


def no_change() -> None:
    """Source files are fine; the operator starts the pipeline from the wrong stage."""


@dataclass(frozen=True)
class Scenario:
    name: str
    mutate: Callable[[], None]
    start_from: str | None = None


SCENARIOS = [
    Scenario("new_column_in_products", new_column_in_products),
    Scenario("text_in_price_column", text_in_price_column),
    Scenario("duplicate_orders", duplicate_orders),
    Scenario("truncated_payments", truncated_payments),
    Scenario("started_from_wrong_stage", no_change, start_from="src.gold"),
]


def latest_run_id() -> str:
    index = json.loads((MONITORING / "index.json").read_text(encoding="utf-8"))
    return sorted(index["runs"], key=lambda r: r["started_at"])[-1]["run_id"]


def build_healthy_state() -> list[str]:
    """Initial load (2026-07-20) and one successful incremental day (2026-07-21)."""

    shutil.rmtree(MONITORING, ignore_errors=True)
    DATA.mkdir(exist_ok=True)
    for child in DATA.iterdir():  # keep the committed data/sample files
        if child.name != "sample":
            shutil.rmtree(child) if child.is_dir() else child.unlink()

    run("-m", "src.data_generator.generate_data")
    run("-m", "src.incremental", "--batch-date", "2026-07-20")
    run("-m", "src.silver", "--batch-date", "2026-07-20")
    run("-m", "src.scd2", "--batch-date", "2026-07-20", "--initialise")
    run("run_pipeline.py", "--batch-date", "2026-07-20", "--start-from", "src.fact_enrichment")
    first = latest_run_id()
    run("-m", "src.data_generator.simulate_next_day", "--batch-date", "2026-07-21")
    run("run_pipeline.py", "--batch-date", "2026-07-21")
    return [first, latest_run_id()]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--yes", action="store_true", help="Confirm that data/ and monitoring/ may be rebuilt")
    parser.add_argument("--only", nargs="*", help="Scenario names to run (default: all)")
    args = parser.parse_args()
    if not args.yes:
        parser.error("this rebuilds data/ and monitoring/; pass --yes to continue")

    selected = [s for s in SCENARIOS if not args.only or s.name in args.only]

    healthy_runs = build_healthy_state()
    shutil.rmtree(BASELINE, ignore_errors=True)
    shutil.copytree(DATA, BASELINE)

    results = {"healthy_runs": healthy_runs, "scenarios": []}
    for scenario in selected:
        print(f"\n{'#' * 100}\nSCENARIO: {scenario.name}\n{'#' * 100}", flush=True)
        shutil.rmtree(DATA)
        shutil.copytree(BASELINE, DATA)
        run("-m", "src.data_generator.simulate_next_day", "--batch-date", SCENARIO_BATCH)
        scenario.mutate()
        command = ["run_pipeline.py", "--batch-date", SCENARIO_BATCH]
        if scenario.start_from:
            command += ["--start-from", scenario.start_from]
        exit_code = run(*command, check=False)
        results["scenarios"].append({"scenario": scenario.name, "run_id": latest_run_id(), "exit_code": exit_code,
                                     "description": (scenario.mutate.__doc__ or "").strip()})

    # Leave data/ in the healthy state.
    shutil.rmtree(DATA)
    shutil.copytree(BASELINE, DATA)
    shutil.rmtree(BASELINE, ignore_errors=True)

    RESULTS.parent.mkdir(exist_ok=True)
    RESULTS.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
