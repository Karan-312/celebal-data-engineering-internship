# Celebal Data Engineering Internship

This repository contains all assignments and projects completed during the **Celebal Technologies Data Engineering Internship** — 8 weeks of progressively advanced data engineering, culminating in a production-grade Databricks Delta Live Tables pipeline.

---

## 📁 Repository Structure

| Folder | Topic |
|---|---|
| `Celebal_data_engineering_assignment_1` | Data Analysis with Python |
| `Celebal_data_engineering_assignment_2` | SQL Database Design & Querying |
| `Celebal_data_engineering_assignment_3` | Advanced SQL (CTEs, Window Functions) |
| `Celebal_data_engineering_assignment_4` | Azure Data Factory & Blob Storage |
| `Celebal_data_engineering_assignment_5` | PySpark Fundamentals |
| `Celebal_data_engineering_assignment_6` | Spark Data Engineering Pipeline |
| `Celebal_data_engineering_assignment_7` | Pandas Data Cleaning |
| `Celebal_data_engineering_assignment_8` | End-to-End Analytics Pipeline (SQLite + SQL) |
| `Customer Sentiment Analysis - final_project` | 🏆 Final Project: Databricks DLT Medallion Pipeline |

---

## 🗂️ Assignment Breakdown

### Assignment 1 — Data Analysis with Python

**Folder:** `Celebal_data_engineering_assignment_1`

Topics covered:
- Data loading and exploration
- Data cleaning and preprocessing
- Exploratory Data Analysis (EDA)
- Business insights generation

Technologies: Python, Pandas, Jupyter Notebook

Files: Dataset, Jupyter Notebook, Analysis Report, README

---

### Assignment 2 — SQL Database Design & Querying

**Folder:** `Celebal_data_engineering_assignment_2`

Topics covered:
- Database design (Primary Keys, Foreign Keys, Constraints, Indexes)
- Filtering (WHERE), Aggregation (GROUP BY), JOIN Operations
- CASE Statements, ACID Properties, Transactions

Technologies: SQL Server, T-SQL

Files: Table Creation Scripts, Data Insertion Scripts, SQL Query Solutions, Documentation

---

### Assignment 3 — Advanced SQL (Subqueries, CTEs & Window Functions)

**Folder:** `Celebal_data_engineering_assignment_3`

Topics covered:
- Subqueries and Correlated Subqueries
- Common Table Expressions (CTEs)
- Window Functions: `RANK()`, `ROW_NUMBER()`, `PARTITION BY`
- Business Analysis: Revenue Analysis, Customer Ranking, SQL-Based Reporting

Key analyses:
- Orders above average sales
- Customer ranking and top-N analysis
- Combined CTE + JOIN + Window Function business insights

Files: Table Creation Scripts, SQL Practice Questions, Mini Project Solutions, Business Insights Documentation

---

### Assignment 4 — Azure Data Factory & Blob Storage Integration

**Folder:** `Celebal_data_engineering_assignment_4`

Topics covered:
- Azure Resource Groups, Storage Accounts, Blob Storage
- Azure Data Factory (ADF) — Linked Services, Datasets, Copy Data Pipelines
- IAM Role Assignments, Pipeline Monitoring, Cloud Data Integration

Key activities:
- Built and executed Copy Data Pipeline from source to destination Blob containers
- Configured Linked Services and IAM permissions
- Bonus: Implemented Get Metadata activity in a metadata-driven pipeline

Technologies: Microsoft Azure, Azure Data Factory, Azure Blob Storage, Azure IAM

---

### Assignment 5 — PySpark Fundamentals

**Folder:** `Celebal_data_engineering_assignment_5`

Topics covered:
- Spark vs. MapReduce — advantages of in-memory processing
- Wide vs. Narrow Transformations and the Shuffle process
- DataFrame Immutability and Schema Inference risks
- Data cleaning: `dropDuplicates`, `na.drop()`, `na.fill()`, null/empty filtering
- Type casting, column renaming, multi-statistic aggregations

Dataset: Synthetically generated e-commerce transaction log (50K+ rows) with injected nulls, empty strings, and duplicates to test edge cases

Technologies: Apache Spark, PySpark, Python 3, Jupyter Notebook

Files: `assignment_spark.ipynb`, `spark_large_dataset.csv`

---

### Assignment 6 — Spark Data Engineering Pipeline

**Folder:** `Celebal_data_engineering_assignment_6`

A full end-to-end PySpark pipeline on a 1,000-record employee dataset — built to deeply understand Spark internals before using them.

Topics covered:
- **Spark Architecture:** Driver, Cluster Manager, Executors
- **Lazy Evaluation & DAG:** Catalyst Optimizer, transformations vs. actions
- **Explicit Schema** with `StructType` (no `inferSchema` risk)
- **Null handling:** single-pass null scan, conditional fill/drop strategies
- **Narrow vs. Wide transformations** — filter/withColumn vs. groupBy/agg
- **Column transformations:** type casting, date parsing, derived columns (`salary_band`, `tenure_years`, `annual_salary`)
- **Predicate Pushdown:** verified via `explain()` that Parquet filters push to the scan
- **CSV vs. Parquet:** columnar storage, compression, partition skipping

Pipeline flow: `CSV → schema inference → null analysis → clean → transform → aggregate → write Parquet (partitioned by department)`

Results: 244 active employees; Sales had highest avg salary; Engineering Parquet partition skipped 5/6 folder trees on read-back

Technologies: PySpark, Python, Parquet, CSV

Files: `spark_pipeline.py`, `employees.csv`

---

### Assignment 7 — Pandas Data Cleaning

**Folder:** `Celebal_data_engineering_assignment_7`

Topics covered:
- Loading and exploring DataFrames: `head()`, `shape`, `dtypes`, `info()`, `describe()`
- Missing value handling: median fill for numerics, mode fill for categoricals, `dropna()` on critical columns
- Row filtering: Technology orders > $100, bulk orders (quantity ≥ 5), column subsets
- Duplicate detection and removal
- Derived column creation: `total_amount = Price × Quantity`

Dataset: Superstore-style dataset (310 rows, 15 columns) with injected missing values and duplicates

Result: Cleaned output — 300 rows × 16 columns, 0 missing values, 0 duplicates

Technologies: Python, Pandas, Jupyter Notebook

Files: `data_cleaning_pandas.ipynb`, `superstore_sample.csv`, `superstore_cleaned.csv`

---

### Assignment 8 — End-to-End E-Commerce Analytics Pipeline

**Folder:** `Celebal_data_engineering_assignment_8`

A complete mini data pipeline covering generation → cleaning → relational modeling → SQL analysis → CLI reporting.

Topics covered:
- **Synthetic data generation** with intentional real-world messiness: duplicates, mixed date formats, orphaned foreign keys, missing PKs, malformed emails, negative quantities
- **Pandas data cleaning** with explicit decision logging to `cleaning_log.txt`
- **Relational database design** in SQLite: 4-table normalized schema with foreign keys and CHECK constraints
- **10 SQL queries** from basic to advanced:
  - Revenue by category, monthly trend with MoM growth (`LAG`)
  - Cumulative revenue (window running total)
  - Top 3 products per category (`RANK() PARTITION BY`)
  - RFM customer segmentation (`NTILE`, nested CTEs)
  - Cohort retention analysis (% of customers still ordering N months later)
  - Customer lifetime value leaderboard (`DENSE_RANK`)
  - Cancellation rate by payment method, repeat vs one-time buyers
- **CLI reporting tool** (`report.py`) — print or export any query as CSV from the terminal

Technologies: Python, Pandas, SQLite, SQL (CTEs, Window Functions, Cohort Analysis)

Files: `data/`, `sql/schema.sql`, `sql/queries.sql`, `scripts/`, `ecommerce.db`, `reports/`

---

## 🏆 Final Project — Near Real-Time Customer Call Sentiment Monitoring Pipeline

**Folder:** `Customer Sentiment Analysis - final_project`

A production-grade **Databricks Delta Live Tables (DLT)** pipeline implementing the full **Medallion Architecture** (Bronze → Silver → Gold) for a telecom company's customer sentiment data.

### Architecture

```
[Synthetic Call Data (JSON)]     [CDC Customer Data (CSV)]
           │                               │
           ▼                               ▼
    bronze_calls                   bronze_customers
   (Materialized View)            (Materialized View)
           │                               │
           ▼                               ├──→ silver_customers_rejects (quarantine)
  silver_calls_clean              silver_customers_clean
  • epoch → event_ts              • CDC type casting
  • deduplication                 • DQ expectations
  • explode transcript segments          │
  • sentiment scoring UDF                ▼
  • flatten metadata struct     silver_customers_scd2
           │                    (SCD Type 2 via apply_changes)
           └───────────────────────────────┤
                                           ▼
               ┌───────────────────────────┼────────────────────────┐
               │                           │                        │
    gold_sentiment_daily      gold_call_volume_daily   gold_high_risk_customers
    gold_region_sentiment     gold_subscription_sentiment
```

### What was built

| Layer | Tables | Description |
|---|---|---|
| **Bronze** | `bronze_calls`, `bronze_customers` | Raw data ingestion with DQ expectations |
| **Silver** | `silver_calls_clean` | Cleaned, deduped, sentiment-scored call events |
| **Silver** | `silver_customers_clean` | Type-cast CDC records |
| **Silver** | `silver_customers_rejects` | Quarantine table for malformed records |
| **Silver** | `silver_customers_scd2` | Full SCD Type 2 history via `dlt.apply_changes()` |
| **Gold** | `gold_sentiment_daily` | Daily positive/negative sentiment ratio |
| **Gold** | `gold_call_volume_daily` | Daily call volume + avg/max/min duration |
| **Gold** | `gold_high_risk_customers` | Customers with ≥3 negative calls in trailing 7 days |
| **Gold** | `gold_region_sentiment` | City-level sentiment breakdown |
| **Gold** | `gold_subscription_sentiment` | Sentiment by subscription tier (Premium/Basic/etc.) |

**Result: All 11 tables ran green. 0 errors. 0 warnings.**

### Technologies Used
- Databricks Free Edition (Serverless)
- Delta Live Tables (DLT / Lakeflow Declarative Pipelines)
- PySpark, Python UDFs
- Unity Catalog (celebal_catalog)
- Delta Lake (SCD Type 2 via `apply_changes`)
- Databricks Asset Bundles (`databricks.yml`)

### Files
- `src/pipeline.py` — consolidated DLT pipeline (single entry point, 491 lines)
- `data_generator/generate_call_stream.py` — synthetic JSON call data generator
- `tests/test_transformations.py` — 17 unit tests
- `docs/` — data dictionary, DAG screenshot

---

## 🛠️ Technologies Used (Full Stack)

| Category | Technologies |
|---|---|
| **Languages** | Python, SQL, T-SQL |
| **Data Processing** | Pandas, PySpark, Apache Spark |
| **Cloud** | Microsoft Azure, Databricks |
| **Databases** | SQL Server, SQLite, Delta Lake |
| **Pipelines** | Azure Data Factory, Databricks DLT |
| **Storage** | Azure Blob Storage, Unity Catalog Volumes, Parquet |
| **Dev Tools** | VS Code, Jupyter Notebook, Git, GitHub |

---

## Internship Track

**Celebal Technologies – Data Engineering Internship**

Assignments organized week-wise, maintained in a single repository for easier tracking and review.

---

## Author

**Karan Rudrawal**
