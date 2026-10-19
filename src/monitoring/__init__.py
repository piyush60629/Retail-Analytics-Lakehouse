"""Run monitoring for the lakehouse pipeline.

Every run of run_pipeline.py writes a folder under monitoring/runs/<run_id>/ with:

    run.json            pipeline and stage status, timings, exit codes, error message, log tail
    source_files.json   source files of the batch: manifest status, checksums, headers vs expected schema
    validation.json     Bronze validation summary and rule failures (quarantined records)
    gold_quality.json   Gold data quality check results
    layer_status.json   which layer outputs exist for the batch

These small JSON files are what an operator (or the Pipeline Incident Agent) reads to
diagnose a failed run without opening Spark logs or Parquet files.
"""
