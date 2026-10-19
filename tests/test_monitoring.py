"""Offline tests for the run log and snapshot helpers (no Spark needed)."""

import json

from src.monitoring import run_log
from src.monitoring.run_log import PipelineRunLog, StageOutput, extract_error, relative_paths
from src.monitoring.snapshot import _content_checks, _file_facts

SPARK_FAILURE = [
    "26/10/19 21:15:15 ERROR Executor: Exception in task 0.0 in stage 29.0 (TID 18)",
    "org.apache.spark.SparkException: [FAILED_READ_FILE.NO_HINT] Encountered error while reading file x.csv.",
    "Caused by: org.apache.spark.SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records",
    "Caused by: java.lang.NumberFormatException: For input string: \"N/A\"",
    "\tat org.apache.spark.sql.catalyst.csv.UnivocityParser.convert(UnivocityParser.scala:1)",
    "Traceback (most recent call last):",
    "py4j.protocol.Py4JJavaError: An error occurred while calling o254.parquet.",
    "src.incremental.bronze_adapter.PipelineExecutionError: Bronze ingestion failed with exit code 1.",
]


def test_root_error_is_the_innermost_cause_not_the_wrapper():
    error = extract_error(SPARK_FAILURE)
    assert error["error_type"] == "src.incremental.bronze_adapter.PipelineExecutionError"
    assert error["root_error"] == 'java.lang.NumberFormatException: For input string: "N/A"'
    assert not any(line.startswith(("Traceback", "26/10/19")) for line in error["key_error_lines"])


def test_root_error_falls_back_to_the_first_python_exception():
    lines = ["src.incremental.bronze_adapter.PipelineExecutionError: failed", "ValueError: orders contains 8 duplicate primary keys.",
             "src.incremental.bronze_adapter.PipelineExecutionError: failed"]
    error = extract_error(lines)
    assert error["root_error"] == "ValueError: orders contains 8 duplicate primary keys."
    assert len(error["key_error_lines"]) == 2  # duplicates removed


def test_paths_are_made_relative():
    root = str(run_log.PROJECT_ROOT)
    assert relative_paths(f"file://{root}/data/x.csv") == "data/x.csv"
    assert relative_paths("/opt/venv/lib/python3.12/site-packages/pyspark/a.py:1") == "site-packages/pyspark/a.py:1"


def test_stage_output_keeps_error_lines_beyond_the_tail():
    output = StageOutput(keep=3)
    output.add("Caused by: java.lang.NumberFormatException: For input string: \"N/A\"\n")
    for _ in range(10):
        output.add("[Stage 3:>   (0 + 1) / 1]")
    assert output.error_lines[0].startswith("Caused by")
    assert output.tail() == []  # progress bars are noise


def test_run_log_records_a_failed_stage_and_skips_the_rest(tmp_path, monkeypatch):
    monkeypatch.setattr(run_log, "RUNS_ROOT", tmp_path)
    stages = [{"name": "Ingest", "module": "a"}, {"name": "Gold", "module": "b"}]
    log = PipelineRunLog("2026-07-22", stages, start_from=None)
    log.stage_started("a")
    output = StageOutput()
    for line in SPARK_FAILURE:
        output.add(line)
    log.stage_finished("a", 1, 2.0, output)
    log.finish("FAILED", 3.0)
    record = json.loads((tmp_path / log.run_id / "run.json").read_text())
    assert record["status"] == "FAILED" and record["failed_stage"] == "Ingest"
    assert record["root_error"].startswith("java.lang.NumberFormatException")
    assert [s["status"] for s in record["stages"]] == ["FAILED", "SKIPPED"]


def test_content_checks_find_bad_numbers_and_duplicate_keys():
    rows = [["product_id", "price"], ["P1", "1.5"], ["P1", "N/A"], ["P2", ""]]
    checks = _content_checks(rows, [("product_id", "string"), ("price", "double")])
    assert checks["values_not_matching_schema_type"] == [
        {"column": "price", "expected_type": "double", "rows": 1, "examples": ["N/A"]}
    ]
    assert checks["duplicate_primary_keys"] == 1


def test_file_facts_detect_a_truncated_file(tmp_path):
    path = tmp_path / "payments.csv"
    path.write_text("payment_id,order_id,amount\nPAY1,ORD1,10.0\nPAY2,ORD2", encoding="utf-8")
    facts = _file_facts(path, [("payment_id", "string"), ("order_id", "string"), ("amount", "double")])
    assert facts["rows_with_wrong_column_count"] == 1 and facts["first_bad_row_numbers"] == [3]  # file line numbers
    assert facts["ends_with_newline"] is False and facts["last_row_preview"] == "PAY2,ORD2"
