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

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Snowflake
- Power BI Dashboard

---

## 👨‍💻 Author

**Piyush Gupta**

Data Engineer | PySpark | Python | SQL | Azure | ETL/ELT | Data Engineering
