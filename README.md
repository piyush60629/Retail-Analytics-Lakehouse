# 🏪 Retail Analytics Lakehouse

**🔗 Live dashboard:** https://retail-lakehouse-piyush.streamlit.app/

An end-to-end **Retail Analytics Lakehouse** built using **PySpark** following the **Medallion Architecture**. The project processes raw retail data through Validation, Bronze, Silver, Incremental ETL, CDC, SCD Type 2, Fact Enrichment, Gold Layer, Data Quality, and Analytics to generate business-ready datasets.

---

## 🚀 Features

- Data Validation & Quarantine
- Bronze Layer (Raw Data)
- Silver Layer (Clean & Standardized Data)
- Incremental ETL Processing
- Change Data Capture (CDC)
- Slowly Changing Dimension (SCD Type 2)
- Fact Enrichment
- Gold Layer (Analytics Ready)
- Gold Data Quality Checks
- Business KPI & Analytics Generation
- Structured run logs and failure snapshots for every pipeline run (`monitoring/`)
- Reproducible failure scenarios, diagnosed by an AI agent ([Pipeline Incident Agent](https://github.com/piyush60629/Pipeline-Incident-Agent))

---

## 🏗 Architecture

```text
Raw Data
   │
   ▼
Validation
   │
   ▼
Bronze
   │
   ▼
Silver
   │
   ▼
Incremental ETL
   │
   ▼
CDC
   │
   ▼
Incremental Silver
   │
   ▼
SCD Type 2
   │
   ▼
Fact Enrichment
   │
   ▼
Gold Layer
   │
   ▼
Data Quality
   │
   ▼
Analytics & KPIs
```

---

## 🛠 Tech Stack

- Python
- PySpark
- Apache Spark
- Faker
- Parquet
- Git & GitHub

---

## 📂 Project Structure

```text
Retail-Analytics-Lakehouse/
│
├── data/
│   └── sample/
├── src/
│   ├── analytics/
│   ├── common/
│   ├── data_generator/
│   ├── fact_enrichment/
│   ├── gold/
│   ├── gold_quality/
│   ├── incremental/
│   ├── incremental_silver/
│   ├── ingestion/
│   ├── monitoring/        # run logs and failure snapshots
│   ├── quality/
│   ├── scd2/
│   ├── silver/
│   └── validation/
│
├── dashboard/
│   ├── app.py
│   ├── export_for_dashboard.py
│   ├── requirements.txt
│   └── data/
├── monitoring/            # one folder per pipeline run (run.json + snapshots)
├── scenarios/             # which failure was injected into which run
├── scripts/
│   └── run_failure_scenarios.py
├── run_pipeline.py
├── requirements.txt
├── .env.example
└── README.md
```

---

## ▶️ Run the Project

Clone the repository:

```bash
git clone https://github.com/piyush60629/Retail-Analytics-Lakehouse.git
cd Retail-Analytics-Lakehouse
```

Install dependencies (Java 17+ is required for PySpark):

```bash
pip install -r requirements.txt
```

**Initial load (first batch)**

```bash
python -m src.data_generator.generate_data
python -m src.incremental --batch-date 2026-07-20
python -m src.silver --batch-date 2026-07-20
python -m src.scd2 --batch-date 2026-07-20 --initialise
python run_pipeline.py --batch-date 2026-07-20 --start-from src.fact_enrichment
```

**Incremental batch (next day)**

`simulate_next_day` changes the source files the way a real system would: segment upgrades, address changes, new customers, price changes, new orders and a few bad rows.

```bash
python -m src.data_generator.simulate_next_day --batch-date 2026-07-21
python run_pipeline.py --batch-date 2026-07-21
```

---

## 📈 Dashboard

The `dashboard/` folder contains a Streamlit app built on the pipeline's real output: executive KPIs, monthly revenue, category and segment revenue, payments, top products and customers, validation results, CDC counts, SCD Type 2 history and all Gold data-quality checks.

After a pipeline run, export the results (small CSVs, no Spark needed) and start the app:

```bash
python -m dashboard.export_for_dashboard --batch-date 2026-07-21
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

The live app is hosted free on Streamlit Community Cloud and reads the exported files in `dashboard/data/`.

---

## 📊 Pipeline Stages

| Stage | Description |
|--------|-------------|
| Validation | Validates incoming data and quarantines invalid records |
| Bronze | Stores raw source data |
| Silver | Cleans and standardizes data |
| Incremental ETL | Processes only new batches |
| CDC | Detects Inserts, Updates and Deletes |
| SCD Type 2 | Maintains historical dimension records |
| Fact Enrichment | Maps facts with dimension surrogate keys |
| Gold | Creates analytics-ready datasets |
| Data Quality | Validates Gold layer outputs |
| Analytics | Generates business KPIs |

---

## 🩺 Run Monitoring and Failure Scenarios

Every `run_pipeline.py` run writes a structured run log, so a failure can be investigated after the fact (by a person or by an AI agent) without rerunning anything:

```text
monitoring/
  index.json                       one line per run: batch date, status, failed stage, error type
  runs/<run_id>/run.json           status, timings, exit code per stage, exception, key error lines, log tail
  runs/<run_id>/source_files.json  raw CSVs: manifest status, rows, rows with the wrong column count,
                                   columns added or missing vs the Bronze schema, values that do not
                                   match numeric types, duplicate primary keys
  runs/<run_id>/validation.json    validation summary and failed data quality rules
  runs/<run_id>/gold_quality.json  Gold reconciliation and integrity checks
  runs/<run_id>/layer_status.json  which Medallion layers have output for the batch date
```

Stage output is still streamed to the console. `--no-monitoring` turns the log off.

### Failure scenarios

`scripts/run_failure_scenarios.py` rebuilds two healthy days (2026-07-20 and 2026-07-21), then runs the real pipeline on batch 2026-07-22 five times, each time with one realistic problem in the input:

| Scenario | What changes | Where the real pipeline fails |
|---|---|---|
| New column in products | The source adds a `currency` column | Bronze load: `MALFORMED_CSV_RECORD` |
| Text in the price column | 60 products arrive with price `N/A` | Bronze load: `NumberFormatException: "N/A"` |
| Duplicate orders | 800 order rows are sent twice | Ingestion: `orders contains 800 duplicate primary keys` |
| Truncated payments file | The transfer stops at 40% of the file | Bronze load: `MALFORMED_CSV_RECORD` on the cut-off last row |
| Started from the wrong stage | `--start-from src.gold` on a new batch | Gold: `FileNotFoundError: SCD2 dimension not found` |

```bash
python scripts/run_failure_scenarios.py --yes      # about 30 minutes; restores the healthy data at the end
```

The mapping from run id to scenario is written to `scenarios/results.json`, separately from `monitoring/`, so the run logs never say what was injected.

**Found while doing this:** if a daily source file is simply not re-delivered, the manifest treats it as unchanged and the pipeline succeeds with the previous day's data. The run log makes this visible (`delivered_for_this_batch: false` in `source_files.json`), but nothing alerts on it yet.

### AI diagnosis with Pipeline Incident Agent

The run logs are read by [Pipeline Incident Agent](https://github.com/piyush60629/Pipeline-Incident-Agent) ([live demo](https://pipeline-incident-agent.streamlit.app/), choose *Retail Analytics Lakehouse*), a LangGraph agent that picks which evidence to check (run log, stage log, source files, validation reports, layer outputs, runbook), writes a structured diagnosis and proposes the correct `--start-from` stage for the rerun, which runs only after a person approves.

---

## 📁 Sample Data

The repository contains sample input files under:

```text
data/sample/
```

Large generated datasets and intermediate pipeline outputs are excluded from Git to keep the repository lightweight.

---

## 📈 Business KPIs

- Total Revenue
- Total Orders
- Average Order Value
- Revenue by Product
- Revenue by Category
- Customer Lifetime Value
- Repeat Customers
- Payment Method Distribution

---

## ☁️ Future Enhancements

- Alert when an expected daily file was not re-delivered
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Snowflake
- Power BI Dashboard

---

## 👨‍💻 Author

**Piyush Gupta**

Data Engineer | PySpark | Python | SQL | Azure | ETL/ELT | Data Engineering
