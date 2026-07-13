"""
gold_high_risk_customers.py — Gold KPI: High-Risk Customer Identification
==========================================================================
Business Question: Which customers need proactive support escalation?

Grain       : 1 row per customer (point-in-time: current trailing 7 days)
Refresh     : Materialized View

Definition of "high-risk"
--------------------------
  A customer is high-risk if they have had ≥ 3 calls with a NEGATIVE
  sentiment label in the trailing 7 days (relative to MAX event_ts seen).

Columns
-------
  customer_id             int     — unique customer identifier
  city                    string  — current city (from SCD2 active row)
  subscription_type       string  — current plan (from SCD2 active row)
  negative_call_count_7d  long    — negative calls in the last 7 days
  last_negative_call_ts   timestamp — most recent negative call timestamp
  total_calls_7d          long    — all calls (any sentiment) in last 7 days
  avg_sentiment_score_7d  double  — average sentiment over last 7 days

Design note
-----------
  The 7-day window is computed relative to MAX(event_ts) in the dataset,
  not wall-clock time. This ensures the KPI remains stable in batch/test runs
  where the latest data may not be "today."

Upstream  : silver_calls_clean, silver_customers_scd2
"""

import dlt
from pyspark.sql.functions import (
    avg, col, count, countDistinct, lit, max as spark_max,
    round as spark_round, to_date, when,
)
from pyspark.sql.window import Window


HIGH_RISK_THRESHOLD = 3  # minimum negative calls in 7 days to be flagged


@dlt.table(
    name="gold_high_risk_customers",
    comment=(
        "High-risk customer identification KPI. "
        f"Flags customers with >= {HIGH_RISK_THRESHOLD} negative calls "
        "in the trailing 7 days for proactive support escalation."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_high_risk_customers():
    calls = dlt.read("silver_calls_clean")
    customers = dlt.read("silver_customers_scd2").filter(col("__end_at").isNull())

    # -- Step 1: Collapse to call-level sentiment ---------------------------
    call_level = (
        calls
        .groupBy("call_id", "customer_id", "event_ts")
        .agg(
            avg("sentiment_score").alias("call_sentiment_score"),
        )
        .withColumn(
            "is_negative",
            when(col("call_sentiment_score") < -0.1, 1).otherwise(0),
        )
    )

    # -- Step 2: Determine the reference timestamp (latest event seen) ------
    max_ts = call_level.agg(spark_max("event_ts").alias("max_ts")).collect()[0]["max_ts"]

    # -- Step 3: Filter to trailing 7 days ----------------------------------
    seven_days_ago = max_ts - (7 * 24 * 60 * 60)  # subtract 7 days in seconds
    recent = call_level.filter(col("event_ts") >= lit(seven_days_ago))

    # -- Step 4: Aggregate per customer -------------------------------------
    agg = (
        recent
        .groupBy("customer_id")
        .agg(
            countDistinct("call_id").alias("total_calls_7d"),
            count(when(col("is_negative") == 1, True)).alias("negative_call_count_7d"),
            spark_max(when(col("is_negative") == 1, col("event_ts"))).alias("last_negative_call_ts"),
            spark_round(avg("call_sentiment_score"), 4).alias("avg_sentiment_score_7d"),
        )
    )

    # -- Step 5: Apply high-risk threshold ----------------------------------
    high_risk = agg.filter(col("negative_call_count_7d") >= HIGH_RISK_THRESHOLD)

    # -- Step 6: Enrich with current customer attributes --------------------
    return (
        high_risk
        .join(
            customers.select("customer_id", "city", "subscription_type", "age"),
            on="customer_id",
            how="left",
        )
        .select(
            "customer_id",
            "city",
            "subscription_type",
            "age",
            "negative_call_count_7d",
            "total_calls_7d",
            "last_negative_call_ts",
            "avg_sentiment_score_7d",
        )
        .orderBy(col("negative_call_count_7d").desc())
    )
