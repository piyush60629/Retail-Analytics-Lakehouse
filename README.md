# 🏪 Retail Analytics Lakehouse

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

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the complete pipeline:

```bash
python run_pipeline.py --batch-date YYYY-MM-DD
```

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
