"""Collect small JSON summaries of a batch's reports next to its run log.

    python -m src.monitoring.snapshot --run-id run-2026-07-22-101500 --batch-date 2026-07-22
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from src.monitoring.run_log import PROJECT_ROOT, RUNS_ROOT

DATA = PROJECT_ROOT / "data"
DATASETS = ["customers", "products", "orders", "order_items", "payments"]
LAYERS = {
    "validated": "validated",
    "bronze": "bronze",
    "cdc": "cdc",
    "silver": "silver",
    "silver_dimensions (SCD2)": "silver_dimensions",
    "enriched_facts": "enriched_facts",
    "gold": "gold",
    "analytics": "analytics",
}


def _schemas() -> dict[str, list[tuple[str, str]]]:
    """(column, Spark type name) per dataset, from the explicit Bronze schemas."""

    from src.ingestion.schemas import DATASET_SCHEMAS

    return {name: [(f.name, f.dataType.typeName()) for f in schema.fields] for name, schema in DATASET_SCHEMAS.items()}


NUMERIC_TYPES = {"integer": int, "long": int, "short": int, "double": float, "float": float}


def _content_checks(rows: list[list[str]], schema: list[tuple[str, str]]) -> dict[str, Any]:
    """Values that will not cast to the schema's numeric types, and duplicate primary keys."""

    if not rows or not schema:
        return {}
    header, body = rows[0], [r for r in rows[1:] if len(r) == len(rows[0])]
    position = {name: index for index, name in enumerate(header)}
    bad_values = []
    for column, type_name in schema:
        cast = NUMERIC_TYPES.get(type_name)
        if cast is None or column not in position:
            continue
        failures = []
        for row in body:
            value = row[position[column]].strip()
            if not value:
                continue
            try:
                cast(value)
            except ValueError:
                failures.append(value)
        if failures:
            bad_values.append({"column": column, "expected_type": type_name, "rows": len(failures),
                               "examples": sorted(set(failures))[:3]})
    key = schema[0][0]
    duplicate_keys = None
    if key in position:
        keys = [row[position[key]] for row in body]
        duplicate_keys = len(keys) - len(set(keys))
    return {"values_not_matching_schema_type": bad_values, "primary_key": key, "duplicate_primary_keys": duplicate_keys}


def _read_spark_csv_folder(folder: Path, limit: int = 200) -> list[dict[str, str]]:
    if not folder.exists():
        return []
    rows: list[dict[str, str]] = []
    for part in sorted(folder.glob("part-*.csv")):
        with part.open(newline="", encoding="utf-8") as handle:
            rows.extend(csv.DictReader(handle))
    return rows[:limit]


def _file_facts(path: Path, schema: list[tuple[str, str]]) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False}
    raw = path.read_bytes()
    text = raw.decode("utf-8", errors="replace")
    rows = list(csv.reader(text.splitlines()))
    header = rows[0] if rows else []
    width = len(header)
    ragged = [index + 1 for index, row in enumerate(rows[1:], start=1) if row and len(row) != width]
    return {
        "exists": True,
        "size_bytes": len(raw),
        "rows_excluding_header": max(0, len(rows) - 1),
        "header": header,
        "rows_with_wrong_column_count": len(ragged),
        "first_bad_row_numbers": ragged[:5],
        "last_row_preview": ",".join(rows[-1])[:200] if len(rows) > 1 else None,
        "ends_with_newline": text.endswith("\n"),
        "md5": hashlib.md5(raw).hexdigest()[:12],
        **_content_checks(rows, schema),
    }


def source_files(batch_date: str) -> dict[str, Any]:
    manifest_path = DATA / "control" / "file_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    schemas = _schemas()
    expected = {name: [column for column, _ in fields] for name, fields in schemas.items()}
    files = []
    for dataset in DATASETS:
        relative = f"{dataset}/{dataset}.csv"
        entry = manifest.get(relative, {})
        facts = _file_facts(DATA / "raw" / relative, schemas.get(dataset, []))
        header = facts.get("header", [])
        files.append({
            "dataset": dataset,
            "path": f"raw/{relative}",
            "manifest_status": entry.get("status"),
            "manifest_batch_date": entry.get("batch_date"),
            # A file that was not re-delivered keeps the manifest entry of an earlier batch.
            "delivered_for_this_batch": entry.get("batch_date") == batch_date if entry else False,
            "manifest_error": entry.get("error_message"),
            **{k: v for k, v in facts.items() if k != "header"},
            "expected_columns": expected.get(dataset, []),
            "received_columns": header,
            "added_columns": [c for c in header if c not in expected.get(dataset, [])],
            "missing_columns": [c for c in expected.get(dataset, []) if c not in header],
        })
    return {"batch_date": batch_date, "files": files}


def validation(batch_date: str) -> dict[str, Any]:
    folder = f"batch_date={batch_date}"
    return {
        "batch_date": batch_date,
        "summary": _read_spark_csv_folder(DATA / "validation_audit" / folder),
        "rule_failures": _read_spark_csv_folder(DATA / "data_quality_report" / folder),
    }


def gold_quality(batch_date: str) -> dict[str, Any]:
    rows = _read_spark_csv_folder(DATA / "gold_quality_audit" / f"batch_date={batch_date}")
    return {
        "batch_date": batch_date,
        "checks_run": len(rows),
        "failed_checks": [r for r in rows if r.get("status") == "FAIL"],
        "passed_checks": sum(1 for r in rows if r.get("status") == "PASS"),
    }


def layer_status(batch_date: str) -> dict[str, Any]:
    folder = f"batch_date={batch_date}"
    status = {}
    for name, directory in LAYERS.items():
        path = DATA / directory / folder
        status[name] = sorted(p.name for p in path.iterdir() if p.is_dir()) if path.exists() else []
    return {"batch_date": batch_date, "outputs_present": status}


def write_snapshot(run_id: str, batch_date: str) -> Path:
    target = RUNS_ROOT / run_id
    target.mkdir(parents=True, exist_ok=True)
    for name, builder in (("source_files", source_files), ("validation", validation),
                          ("gold_quality", gold_quality), ("layer_status", layer_status)):
        try:
            payload = builder(batch_date)
        except Exception as exc:  # noqa: BLE001 - a snapshot must never hide the run's own result
            payload = {"batch_date": batch_date, "snapshot_error": f"{type(exc).__name__}: {exc}"}
        (target / f"{name}.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    update_index()
    return target


def update_index() -> Path:
    runs = []
    for run_file in sorted(RUNS_ROOT.glob("*/run.json")):
        record = json.loads(run_file.read_text(encoding="utf-8"))
        runs.append({k: record.get(k) for k in ("run_id", "batch_date", "status", "started_at",
                                                 "duration_seconds", "failed_stage", "error_type")})
    index = RUNS_ROOT.parent / "index.json"
    index.write_text(json.dumps({"runs": sorted(runs, key=lambda r: r["started_at"] or "")}, indent=2) + "\n",
                     encoding="utf-8")
    return index


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()
    print(write_snapshot(args.run_id, args.batch_date))


if __name__ == "__main__":
    main()
