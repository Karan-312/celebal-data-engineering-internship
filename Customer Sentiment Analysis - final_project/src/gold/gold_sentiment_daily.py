"""
gold_sentiment_daily.py — Gold KPI: Daily Sentiment Distribution
=================================================================
Business Question: Is overall customer sentiment trending up or down?

Grain       : 1 row per calendar day
Refresh     : Materialized View (incremental refresh on each pipeline run)

Columns
-------
  date              date    — calendar date of the calls
  positive_calls    long    — count of calls with sentiment_label = 'positive'
  negative_calls    long    — count of calls with sentiment_label = 'negative'
  neutral_calls     long    — count of calls with sentiment_label = 'neutral'
  total_calls       long    — total distinct calls that day
  sentiment_ratio   double  — positive_calls / total_calls (0.0–1.0)
  avg_sentiment_score double — mean sentiment_score across all calls

Notes
-----
- We join silver_calls_clean with the CURRENT SCD2 row (__end_at IS NULL).
  This is intentional for Gold KPIs that reflect the customer's latest
  attributes. For point-in-time analysis, use silver_customers_scd2 directly.
- Aggregation is at call level (DISTINCT call_id), not segment level,
  because silver_calls_clean is exploded (one row per transcript segment).
  We use MAX(sentiment_score) per call to represent the dominant sentiment.

Upstream  : silver_calls_clean, silver_customers_scd2
"""

import dlt
from pyspark.sql.functions import (
    avg, col, count, countDistinct, sum as spark_sum,
    to_date, when, round as spark_round,
)


@dlt.table(
    name="gold_sentiment_daily",
    comment=(
        "Daily sentiment distribution KPI. "
        "Tracks ratio of positive to negative calls each day for trend detection."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_sentiment_daily():
    # -- Join calls with current customer snapshot --------------------------
    calls = dlt.read("silver_calls_clean")
    customers = dlt.read("silver_customers_scd2").filter(col("__end_at").isNull())

    enriched = calls.join(customers, on="customer_id", how="left")

    # -- Call-level aggregation (collapse segments back to call grain) ------
    call_level = (
        enriched
        .groupBy("call_id", to_date("event_ts").alias("date"))
        .agg(
            avg("sentiment_score").alias("call_sentiment_score"),
            # Take the most frequent label per call via max (simple heuristic)
            # In production, use mode() or a custom UDAF.
        )
        .withColumn(
            "call_sentiment_label",
            when(col("call_sentiment_score") > 0.1, "positive")
            .when(col("call_sentiment_score") < -0.1, "negative")
            .otherwise("neutral"),
        )
    )

    # -- Daily aggregation --------------------------------------------------
    return (
        call_level
        .groupBy("date")
        .agg(
            countDistinct("call_id").alias("total_calls"),
            spark_sum(
                when(col("call_sentiment_label") == "positive", 1).otherwise(0)
            ).alias("positive_calls"),
            spark_sum(
                when(col("call_sentiment_label") == "negative", 1).otherwise(0)
            ).alias("negative_calls"),
            spark_sum(
                when(col("call_sentiment_label") == "neutral", 1).otherwise(0)
            ).alias("neutral_calls"),
            spark_round(avg("call_sentiment_score"), 4).alias("avg_sentiment_score"),
        )
        .withColumn(
            "sentiment_ratio",
            spark_round(
                col("positive_calls") / col("total_calls").cast("double"), 4
            ),
        )
        .orderBy("date")
    )
