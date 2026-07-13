"""
pipeline.py — Consolidated Databricks Lakeflow Declarative Pipeline
====================================================================
This is the SINGLE FILE referenced by databricks.yml as the DLT library.
It imports and registers all Bronze, Silver, and Gold table definitions.

Why a single file for DLT?
---------------------------
Databricks DLT resolves the dependency graph (DAG) automatically across
all @dlt.table definitions in a pipeline. By consolidating here, we get:
  1. A single entry point for the Asset Bundle (simpler databricks.yml).
  2. Clear dependency ordering via the `import` structure.
  3. The modular files (src/bronze/, src/silver/, src/gold/) remain as
     readable, individually-documented source files for GitHub.

DAG produced by this pipeline
------------------------------
                    [bronze_calls]          [bronze_customers]
                         |                        |
                  [silver_calls_clean]   [silver_customers_clean]
                         |                        |
                         |             [silver_customers_rejects] (quarantine)
                         |                        |
                         |             [silver_customers_scd2]  (SCD Type 2)
                         |                        |
                         +────────────────────────+
                                       |
              ┌────────────────────────┼────────────────────────┐
              │                        │                        │
   [gold_sentiment_daily]   [gold_call_volume_daily]   [gold_high_risk_customers]
   [gold_region_sentiment]  [gold_subscription_sentiment]

Configuration (injected via databricks.yml pipeline.configuration)
-------------------------------------------------------------------
  calls_landing_path      : Volume path for JSON call files
  customers_landing_path  : Volume path for CDC CSV files
  catalog                 : Unity Catalog name
  schema                  : Target schema (dev vs prod)

Usage
-----
  # Deploy via Databricks CLI
  databricks bundle deploy --target development
  databricks bundle run sentiment_pipeline

  # Or trigger manually in the Databricks UI → Workflows → Pipelines
"""

# ============================================================================
# Imports — order matters for readability; DLT resolves deps automatically
# ============================================================================

# Spark / DLT
import dlt
from pyspark.sql.functions import (
    avg, col, count, countDistinct, current_timestamp, expr, explode,
    from_unixtime, lit, max as spark_max, min as spark_min,
    round as spark_round, sum as spark_sum, to_date, to_timestamp, when,
)
from pyspark.sql.types import FloatType, StringType
from pyspark.sql.functions import udf

# ============================================================================
# Shared Configuration
# ============================================================================

CALLS_LANDING_PATH = spark.conf.get(
    "calls_landing_path",
    "/Volumes/celebal_catalog/sentiment_pipeline/landing/calls",
)
CUSTOMERS_LANDING_PATH = spark.conf.get(
    "customers_landing_path",
    "/Volumes/celebal_catalog/sentiment_pipeline/landing/customers",
)
# Schema location is managed internally by DLT — no manual path needed

HIGH_RISK_THRESHOLD = 3  # minimum negative calls in trailing 7 days

# ============================================================================
# Sentiment Scoring (inline — avoids import path issues in DLT runtime)
# ============================================================================

import re

POSITIVE_WORDS = {
    "excellent", "great", "happy", "satisfied", "resolved", "helpful",
    "perfect", "amazing", "wonderful", "fantastic", "appreciate", "thanks",
    "thank you", "pleased", "good", "nice", "love", "impressed", "awesome",
    "outstanding", "efficient", "quick", "fast", "smooth", "easy", "works",
    "fixed", "sorted", "solved", "glad", "delighted", "superb", "brilliant",
}

NEGATIVE_WORDS = {
    "frustrated", "disappointed", "terrible", "awful", "horrible", "bad",
    "worst", "waste", "broken", "failed", "unacceptable", "angry", "upset",
    "disgusted", "useless", "incompetent", "never", "always", "escalate",
    "refund", "cancel", "complaint", "problem", "issue", "error", "bug",
    "disconnected", "dropped", "slow", "delayed", "missing", "wrong",
    "ridiculous", "absurd", "scam", "fraud", "lied", "cheated",
}

INTENSIFIERS = {"very", "extremely", "absolutely", "completely", "totally"}


def _score_text(text: str) -> float:
    if not text:
        return 0.0
    tokens = re.findall(r"[a-z']+", text.lower())
    score = 0.0
    weight = 1.0
    for token in tokens:
        if token in INTENSIFIERS:
            weight = 2.0
            continue
        if token in POSITIVE_WORDS:
            score += weight
        elif token in NEGATIVE_WORDS:
            score -= weight
        weight = 1.0
    if score == 0:
        return 0.0
    norm = score / (abs(score) + 1.0)
    return round(max(-1.0, min(1.0, norm)), 4)


def _score_label(score: float) -> str:
    if score is None:
        return "neutral"
    if score > 0.1:
        return "positive"
    elif score < -0.1:
        return "negative"
    return "neutral"


SENTIMENT_SCORE_UDF = udf(_score_text, FloatType())
SENTIMENT_LABEL_UDF = udf(_score_label, StringType())


# ============================================================================
# BRONZE LAYER
# ============================================================================

@dlt.table(
    name="bronze_calls",
    comment=(
        "Raw call events from Auto Loader JSON stream. "
        "Append-only; zero transformation. "
        "Full lineage via source_file + ingestion_timestamp."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect("valid_call_id",          "call_id IS NOT NULL")
@dlt.expect("valid_customer_id",      "customer_id IS NOT NULL")
@dlt.expect("valid_epoch",            "epoch_timestamp IS NOT NULL")
@dlt.expect("non_negative_duration",  "duration_sec IS NULL OR duration_sec >= 0")
def bronze_calls():
    # Reads from the raw_calls Delta table pre-created via SQL Editor.
    # (DLT ETL compute cannot resolve /Volumes/ paths directly; SQL Warehouse can.)
    return spark.table("celebal_catalog.sentiment_pipeline.raw_calls")


@dlt.table(
    name="bronze_customers",
    comment=(
        "Raw CDC customer profile records from batch CSV. "
        "Full pass-through including operation and update_ts."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect("valid_customer_id",  "customer_id IS NOT NULL")
@dlt.expect("valid_operation",    "operation IN ('INSERT','UPDATE','DELETE')")
@dlt.expect("valid_update_ts",    "update_ts IS NOT NULL")
def bronze_customers():
    # Reads from the raw_customers Delta table pre-created via SQL Editor.
    return spark.table("celebal_catalog.sentiment_pipeline.raw_customers")


# ============================================================================
# SILVER LAYER — Calls
# ============================================================================

@dlt.table(
    name="silver_calls_clean",
    comment=(
        "Cleaned, deduplicated, and sentiment-enriched call events. "
        "One row per transcript segment. "
        "Watermarked on event_ts with 30-minute late-data tolerance."
    ),
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_or_drop("non_negative_duration", "duration_sec >= 0")
@dlt.expect_or_drop("valid_event_ts",         "event_ts IS NOT NULL")
@dlt.expect_or_drop("valid_call_id",          "call_id IS NOT NULL")
def silver_calls_clean():
    bronze = dlt.read("bronze_calls")

    # 1. epoch -> event_ts
    with_ts = bronze.withColumn(
        "event_ts",
        to_timestamp(from_unixtime(col("epoch_timestamp")))
    )

    # 2. Composite-key deduplication (batch mode: no watermark needed)
    deduped = with_ts.dropDuplicates(["call_id", "event_ts"])

    # 3. Flatten metadata struct
    flattened = (
        deduped
        .withColumn("call_type", col("metadata.call_type"))
        .withColumn("channel",   col("metadata.channel"))
        .withColumn("region",    col("metadata.region"))
        .drop("metadata")
    )

    # 4. Explode transcript_segments
    exploded = (
        flattened
        .withColumn("segment",   explode(col("transcript_segments")))
        .withColumn("speaker",   col("segment.speaker"))
        .withColumn("text",      col("segment.text"))
        .withColumn("start_sec", col("segment.start_sec"))
        .drop("transcript_segments", "segment")
    )

    # 5. Sentiment scoring
    enriched = (
        exploded
        .withColumn("sentiment_score", SENTIMENT_SCORE_UDF(col("text")))
        .withColumn("sentiment_label", SENTIMENT_LABEL_UDF(col("sentiment_score")))
    )

    # 6. Drop generator debug markers
    final_cols = [c for c in enriched.columns if c != "_injection_type"]
    return enriched.select(final_cols)


# ============================================================================
# SILVER LAYER — Customers (CDC + SCD Type 2)
# ============================================================================

@dlt.table(
    name="silver_customers_clean",
    comment="Valid CDC customer records after quarantine. Malformed rows go to silver_customers_rejects.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_operation",   "operation IN ('INSERT','UPDATE','DELETE')")
@dlt.expect_or_drop("valid_update_ts",   "update_ts IS NOT NULL")
def silver_customers_clean():
    return (
        dlt.read("bronze_customers")
        .withColumn("customer_id",  col("customer_id").cast("int"))
        .withColumn("age",          col("age").cast("int"))
        .withColumn("signup_date",  col("signup_date").cast("date"))
        .withColumn("update_ts",    col("update_ts").cast("timestamp"))
    )


@dlt.table(
    name="silver_customers_rejects",
    comment="Quarantine: malformed CDC records excluded from Silver processing.",
    table_properties={"quality": "quarantine"},
)
def silver_customers_rejects():
    return (
        dlt.read("bronze_customers")
        .filter(
            col("customer_id").isNull()
            | ~col("operation").isin("INSERT", "UPDATE", "DELETE")
            | col("update_ts").isNull()
        )
        .withColumn("reject_reason",
                    expr("""
                        CASE
                          WHEN customer_id IS NULL THEN 'null_customer_id'
                          WHEN operation NOT IN ('INSERT','UPDATE','DELETE') THEN 'invalid_operation'
                          WHEN update_ts IS NULL THEN 'null_update_ts'
                          ELSE 'unknown'
                        END
                    """))
    )


# SCD Type 2 — Native DLT apply_changes (no hand-rolled MERGE)
# create_streaming_table MUST be declared before apply_changes in DLT
dlt.create_streaming_table(
    name="silver_customers_scd2",
    comment="SCD Type 2 full history of customer attributes (city, subscription_type). Managed by dlt.apply_changes().",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)

dlt.apply_changes(
    target="silver_customers_scd2",
    source="silver_customers_clean",
    keys=["customer_id"],
    sequence_by=col("update_ts"),
    stored_as_scd_type=2,
    track_history_column_list=["city", "subscription_type"],
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation"],
)


# ============================================================================
# GOLD LAYER — KPI Materialized Views
# Helper: enriched call-level dataframe (reused across all Gold tables)
# ============================================================================

def _call_level_with_customers():
    """
    Internal helper: collapse segment-level silver_calls_clean back to
    call-level sentiment, then join with current SCD2 customer attributes.
    """
    calls = dlt.read("silver_calls_clean")
    customers = dlt.read("silver_customers_scd2").filter(col("__end_at").isNull())

    call_level = (
        calls
        .groupBy(
            "call_id", "customer_id", "duration_sec",
            to_date("event_ts").alias("date"),
            "event_ts",
        )
        .agg(avg("sentiment_score").alias("call_sentiment_score"))
        .withColumn(
            "call_label",
            when(col("call_sentiment_score") > 0.1, "positive")
            .when(col("call_sentiment_score") < -0.1, "negative")
            .otherwise("neutral"),
        )
    )

    return call_level.join(
        customers.select("customer_id", "city", "subscription_type", "age"),
        on="customer_id",
        how="left",
    )


# ── KPI 1: Daily Sentiment Distribution ─────────────────────────────────────

@dlt.table(
    name="gold_sentiment_daily",
    comment="Daily sentiment KPI: tracks positive/negative/neutral call ratios per day.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def gold_sentiment_daily():
    enriched = _call_level_with_customers()
    return (
        enriched
        .groupBy("date")
        .agg(
            countDistinct("call_id").alias("total_calls"),
            spark_sum(when(col("call_label") == "positive", 1).otherwise(0)).alias("positive_calls"),
            spark_sum(when(col("call_label") == "negative", 1).otherwise(0)).alias("negative_calls"),
            spark_sum(when(col("call_label") == "neutral",  1).otherwise(0)).alias("neutral_calls"),
            spark_round(avg("call_sentiment_score"), 4).alias("avg_sentiment_score"),
        )
        .withColumn(
            "sentiment_ratio",
            spark_round(col("positive_calls") / col("total_calls").cast("double"), 4),
        )
        .orderBy("date")
    )


# ── KPI 2: Daily Call Volume ─────────────────────────────────────────────────

@dlt.table(
    name="gold_call_volume_daily",
    comment="Daily call volume KPI: total calls and duration stats per day.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def gold_call_volume_daily():
    calls = dlt.read("silver_calls_clean")
    call_level = (
        calls
        .groupBy("call_id", to_date("event_ts").alias("date"))
        .agg(spark_max("duration_sec").alias("duration_sec"))
    )
    return (
        call_level
        .groupBy("date")
        .agg(
            countDistinct("call_id").alias("total_calls"),
            spark_round(avg("duration_sec"), 1).alias("avg_duration_sec"),
            spark_max("duration_sec").alias("max_duration_sec"),
            spark_min("duration_sec").alias("min_duration_sec"),
        )
        .orderBy("date")
    )


# ── KPI 3: High-Risk Customers ────────────────────────────────────────────────

@dlt.table(
    name="gold_high_risk_customers",
    comment=f"High-risk customer KPI: customers with >= {HIGH_RISK_THRESHOLD} negative calls in trailing 7 days.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def gold_high_risk_customers():
    enriched = _call_level_with_customers()

    # Use Spark SQL interval for the 7-day window — avoids Python NoneType crash
    # when max_ts is None (empty or fully-null event_ts column)
    recent = enriched.filter(
        col("event_ts") >= expr("current_timestamp() - INTERVAL 7 DAYS")
    )

    return (
        recent
        .groupBy("customer_id", "city", "subscription_type", "age")
        .agg(
            countDistinct("call_id").alias("total_calls_7d"),
            count(when(col("call_label") == "negative", True)).alias("negative_call_count_7d"),
            spark_max(when(col("call_label") == "negative", col("event_ts"))).alias("last_negative_call_ts"),
            spark_round(avg("call_sentiment_score"), 4).alias("avg_sentiment_score_7d"),
        )
        .filter(col("negative_call_count_7d") >= HIGH_RISK_THRESHOLD)
        .orderBy(col("negative_call_count_7d").desc())
    )


# ── KPI 4: Region-wise Sentiment ─────────────────────────────────────────────

@dlt.table(
    name="gold_region_sentiment",
    comment="Region sentiment KPI: sentiment breakdown by city and day.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def gold_region_sentiment():
    enriched = _call_level_with_customers()
    return (
        enriched
        .groupBy("city", "date")
        .agg(
            countDistinct("call_id").alias("total_calls"),
            spark_round(avg("call_sentiment_score"), 4).alias("avg_sentiment_score"),
            spark_sum(when(col("call_label") == "positive", 1).otherwise(0)).alias("positive_calls"),
            spark_sum(when(col("call_label") == "negative", 1).otherwise(0)).alias("negative_calls"),
            spark_sum(when(col("call_label") == "neutral",  1).otherwise(0)).alias("neutral_calls"),
        )
        .withColumn(
            "sentiment_ratio",
            spark_round(col("positive_calls") / col("total_calls").cast("double"), 4),
        )
        .orderBy("date", "city")
    )


# ── KPI 5: Subscription-based Sentiment ──────────────────────────────────────

@dlt.table(
    name="gold_subscription_sentiment",
    comment="Subscription sentiment KPI: sentiment breakdown by plan tier and day.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def gold_subscription_sentiment():
    enriched = _call_level_with_customers()
    return (
        enriched
        .groupBy("subscription_type", "date")
        .agg(
            countDistinct("call_id").alias("total_calls"),
            spark_round(avg("call_sentiment_score"), 4).alias("avg_sentiment_score"),
            spark_sum(when(col("call_label") == "positive", 1).otherwise(0)).alias("positive_calls"),
            spark_sum(when(col("call_label") == "negative", 1).otherwise(0)).alias("negative_calls"),
            spark_sum(when(col("call_label") == "neutral",  1).otherwise(0)).alias("neutral_calls"),
        )
        .withColumn(
            "sentiment_ratio",
            spark_round(col("positive_calls") / col("total_calls").cast("double"), 4),
        )
        .orderBy("date", "subscription_type")
    )
