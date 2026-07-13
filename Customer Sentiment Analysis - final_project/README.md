# 🚀 Near Real-Time Customer Call Sentiment Monitoring Pipeline

> **Celebal Technologies Final Project** | Databricks Lakeflow Declarative Pipelines (DLT) | Medallion Architecture

[![CI](https://github.com/your-username/celebal-data-engineering/actions/workflows/ci.yml/badge.svg)](https://github.com/your-username/celebal-data-engineering/actions)

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│  [JSON Stream via Auto Loader]      [CDC CSV Batch - INSERT/UPDATE/DELETE] │
│   generate_call_stream.py            customer_cdc_data_final.csv    │
└──────────────┬──────────────────────────────┬───────────────────────┘
               │                              │
               ▼                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER (Raw)                            │
│   bronze_calls (Streaming Table)    bronze_customers (Streaming Table)│
│   • Auto Loader cloudFiles format   • CDC CSV pass-through           │
│   • source_file + ingestion_ts      • operation + update_ts columns  │
│   • @dlt.expect (warn-only)         • zero transformation            │
└──────────────┬──────────────────────────────┬───────────────────────┘
               │                              │
               ▼                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER (Cleaned)                        │
│   silver_calls_clean                silver_customers_scd2            │
│   • epoch → event_ts                • dlt.apply_changes (SCD Type 2) │
│   • withWatermark(30 min)           • Tracks: city, subscription_type│
│   • deduplicate (call_id+event_ts)  • __start_at / __end_at columns  │
│   • explode transcript_segments     • silver_customers_rejects table  │
│   • flatten metadata struct         • sequenced by update_ts          │
│   • compute sentiment_score/label   • DELETE handling                 │
└──────────────┬──────────────────────────────┬───────────────────────┘
               │                              │
               └──────────────┬───────────────┘
                              │ (joined on customer_id, __end_at IS NULL)
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                   GOLD LAYER (Business KPIs)                         │
│   gold_sentiment_daily     gold_call_volume_daily                    │
│   gold_high_risk_customers gold_region_sentiment                     │
│   gold_subscription_sentiment                                        │
│   All: Materialized Views, refreshed incrementally                   │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 🗂️ Repository Structure

```
celebal-data-engineering/
├── README.md
├── databricks.yml                        ← Asset Bundle: deploy-as-code
├── data_generator/
│   └── generate_call_stream.py           ← Simulates live JSON call source
├── src/
│   ├── pipeline.py                       ← Consolidated DLT entry point
│   ├── utils/
│   │   └── sentiment.py                  ← Shared sentiment scoring logic
│   ├── bronze/
│   │   ├── bronze_calls.py
│   │   └── bronze_customers.py
│   ├── silver/
│   │   ├── silver_calls_clean.py
│   │   └── silver_customers_scd2.py
│   └── gold/
│       ├── gold_sentiment_daily.py
│       ├── gold_call_volume_daily.py
│       ├── gold_high_risk_customers.py
│       ├── gold_region_sentiment.py
│       └── gold_subscription_sentiment.py
├── tests/
│   └── test_transformations.py
├── docs/
│   ├── dag_screenshot.png                ← Capture from Databricks UI
│   └── data_dictionary.md
└── .github/
    └── workflows/
        └── ci.yml
```

---

## 🔧 Setup Instructions

### Prerequisites
- Databricks Free Edition workspace
- Databricks CLI installed (`pip install databricks-cli`)
- Personal Access Token (PAT) from Databricks settings
- VS Code + Databricks extension

### Step 1: Databricks Workspace Setup

1. Create a Catalog: `celebal_catalog`
2. Create a Schema: `sentiment_pipeline`
3. Create a Volume: `landing` under the schema above
4. Upload `customer_cdc_data_final.csv` to `/Volumes/celebal_catalog/sentiment_pipeline/landing/customers/`

### Step 2: Configure CLI Authentication

```bash
databricks configure --token
# Enter: workspace URL + PAT
```

### Step 3: Upload Data Generator Output

```bash
python data_generator/generate_call_stream.py
# Files will be written to ./output/calls/*.json
# Upload these to /Volumes/celebal_catalog/sentiment_pipeline/landing/calls/
```

### Step 4: Deploy the Pipeline

```bash
databricks bundle deploy --target development
databricks bundle run sentiment_pipeline
```

---

## 📊 Gold Layer KPI Reference

| Table | Grain | Key Columns | Business Question |
|---|---|---|---|
| `gold_sentiment_daily` | 1 row / day | `date`, `positive_calls`, `negative_calls`, `sentiment_ratio` | Is sentiment trending up or down? |
| `gold_call_volume_daily` | 1 row / day | `date`, `total_calls`, `avg_duration_sec` | When are traffic peaks? |
| `gold_high_risk_customers` | 1 row / customer | `customer_id`, `negative_call_count_7d`, `last_negative_call_ts` | Who needs proactive escalation? |
| `gold_region_sentiment` | 1 row / city / day | `city`, `date`, `avg_sentiment_score` | Which regions have worse experience? |
| `gold_subscription_sentiment` | 1 row / tier / day | `subscription_type`, `date`, `avg_sentiment_score` | Are Premium customers happier? |

---

## 🏗️ Design Decisions

### Why `dlt.apply_changes()` for SCD Type 2?
Using the native DLT API instead of hand-rolled MERGE statements demonstrates real platform fluency. It handles out-of-order CDC events automatically via `sequence_by`, requires no custom state management, and produces audit-ready `__start_at`/`__end_at` columns.

### Why event-time watermarking (not processing-time)?
Mobile and network delays mean calls frequently arrive 5–30 minutes late. Anchoring deduplication to `event_ts` (when the call happened) rather than ingestion time prevents silent data corruption where the same call could be counted twice during late arrival.

### Why keyword-based sentiment scoring?
The pipeline is designed to swap in any NLP model. The `src/utils/sentiment.py` module exposes a `score_text(text: str) -> float` function that currently uses a curated positive/negative keyword dictionary. To upgrade to VADER or a transformer model, only this function changes — the DLT table definitions remain untouched.

### Why a separate reject table for malformed customer records?
Silent drops hide data quality problems. By routing invalid rows to `silver_customers_rejects` via `@dlt.expect_or_drop`, operators can audit exactly which records failed and why, without halting the pipeline.

---

## 🧪 Running Tests

```bash
pip install pytest
pytest tests/ -v
```

---

## 📋 Data Dictionary

See [docs/data_dictionary.md](docs/data_dictionary.md) for full column-level documentation.
