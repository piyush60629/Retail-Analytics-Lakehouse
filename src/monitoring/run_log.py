"""Structured run log for run_pipeline.py: one JSON file per pipeline run."""

from __future__ import annotations

import json
import os
import re
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MONITORING_ROOT = PROJECT_ROOT / os.getenv("MONITORING_DIR", "monitoring")
RUNS_ROOT = MONITORING_ROOT / "runs"

LOG_TAIL_LINES = 40
# Spark progress bars and blank lines add nothing to a log tail.
NOISE = re.compile(r"^\s*$|^\[Stage \d+|^\s*\[Stage|^=+$|^-+$")
ERROR_LINE = re.compile(
    r"^(?:Traceback|Caused by:|[\w.]*(?:Error|Exception)\b[:\s]|.*\bERROR\b|py4j\.protocol\.Py4JJavaError)"
)
EXCEPTION_LINE = re.compile(r"^(?P<type>[A-Za-z_][\w.]*(?:Error|Exception))\s*:\s*(?P<message>.*)$")


def relative_paths(text: str) -> str:
    """Show paths relative to the project, so logs read the same on any machine."""

    root = str(PROJECT_ROOT)
    return text.replace(f"file://{root}/", "").replace(f"{root}/", "")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def extract_error(lines: Iterable[str]) -> dict[str, Any]:
    """Pull the final exception and the key error lines out of a stage's output."""

    lines = [relative_paths(line.rstrip()) for line in lines]
    error_type, error_message = None, None
    for line in lines:
        match = EXCEPTION_LINE.match(line.strip())
        if match:
            error_type, error_message = match.group("type"), match.group("message").strip()
    key_lines = [line.strip()[:400] for line in lines if ERROR_LINE.match(line.strip())]
    return {
        "error_type": error_type,
        "error_message": (error_message or "")[:800] or None,
        "key_error_lines": key_lines[-10:],
    }


class StageOutput:
    """Keeps the last lines of a stage's output, plus every error-looking line, because long
    Spark stack traces can push the important 'Caused by' lines out of a plain tail."""

    def __init__(self, keep: int = 400) -> None:
        self.lines: deque[str] = deque(maxlen=keep)
        self.error_lines: deque[str] = deque(maxlen=200)

    def add(self, line: str) -> None:
        line = line.rstrip("\n")
        self.lines.append(line)
        if ERROR_LINE.match(line.strip()):
            self.error_lines.append(line)

    def tail(self, count: int = LOG_TAIL_LINES) -> list[str]:
        useful = [relative_paths(line)[:400] for line in self.lines if not NOISE.match(line)]
        return useful[-count:]


class PipelineRunLog:
    def __init__(self, batch_date: str, stages: list[dict[str, str]], start_from: str | None) -> None:
        started = datetime.now(timezone.utc)
        run_id = f"run-{batch_date}-{started:%H%M%S}"
        suffix = 1
        while (RUNS_ROOT / run_id).exists():
            suffix += 1
            run_id = f"run-{batch_date}-{started:%H%M%S}-{suffix}"
        self.run_id = run_id
        self.directory = RUNS_ROOT / self.run_id
        self.record: dict[str, Any] = {
            "run_id": self.run_id,
            "pipeline": "retail_analytics_lakehouse",
            "batch_date": batch_date,
            "status": "RUNNING",
            "started_at": started.isoformat(timespec="seconds"),
            "finished_at": None,
            "duration_seconds": None,
            "start_from": start_from,
            "triggered_by": os.getenv("PIPELINE_TRIGGERED_BY", "manual"),
            "failed_stage": None,
            "error_type": None,
            "error_message": None,
            "stages": [
                {"name": s["name"], "module": s["module"], "status": "PENDING", "started_at": None,
                 "duration_seconds": None, "exit_code": None}
                for s in stages
            ],
        }
        self._started_monotonic = None

    def _stage(self, module: str) -> dict[str, Any]:
        return next(s for s in self.record["stages"] if s["module"] == module)

    def stage_started(self, module: str) -> None:
        stage = self._stage(module)
        stage["status"], stage["started_at"] = "RUNNING", utc_now()
        self.save()

    def stage_finished(self, module: str, exit_code: int, seconds: float, output: StageOutput) -> None:
        stage = self._stage(module)
        stage.update(status="SUCCEEDED" if exit_code == 0 else "FAILED", exit_code=exit_code,
                     duration_seconds=round(seconds, 1))
        if exit_code != 0:
            stage.update(extract_error([*output.error_lines, *output.lines]))
            stage["log_tail"] = output.tail()
            self.record.update(failed_stage=stage["name"], error_type=stage.get("error_type"),
                               error_message=stage.get("error_message"))
        self.save()

    def finish(self, status: str, seconds: float) -> None:
        for stage in self.record["stages"]:
            if stage["status"] == "PENDING":
                stage["status"] = "SKIPPED"
        self.record.update(status=status, finished_at=utc_now(), duration_seconds=round(seconds, 1))
        self.save()

    def save(self) -> Path:
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / "run.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.record, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
        return path
