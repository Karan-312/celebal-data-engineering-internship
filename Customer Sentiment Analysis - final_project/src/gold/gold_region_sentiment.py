"""
gold_region_sentiment.py — Gold KPI: Region-wise Sentiment
===========================================================
Business Question: Which regions have worse service experience?

Grain       : 1 row per city per calendar day
Refresh     : Materialized View

Columns
-------
  city                string  — city name from SCD2 active row
  date                date    — calendar date of the calls
  total_calls         long    — distinct calls in this city on this date
  avg_sentiment_score double  — mean sentiment score (higher = more positive)
  positive_calls      long    — positive-sentiment calls
  negative_calls      long    — negative-sentiment calls
  neutral_calls       long    — neutral-sentiment calls
  sentiment_ratio     double  — positive_calls / total_calls

Design note
-----------
  City is sourced from silver_customers_scd2 (active row, __end_at IS NULL),
  NOT from the metadata.region column in call records.

  This is intentional: if a customer moved from Delhi to Mumbai, calls made
  before the move are attributed to Delhi (via SCD2 start/end dates in point-
  in-time analysis), but for this Gold KPI we use the CURRENT city to show
  where the customer is now. For retroactive regional analysis, a separate
  point-in-time join would be needed.

Upstream  : silver_calls_clean, silver_customers_scd2
"""

import dlt
from pyspark.sql.functions import (
    avg, col, countDistinct, round as spark_round,
    sum as spark_sum, to_date, when,
)


@dlt.table(
    name="gold_region_sentiment",
    comment=(
        "Region-wise sentiment KPI. "
        "Breaks down sentiment scores by city to reveal geographic service disparities."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_region_sentiment():
    calls = dlt.read("silver_calls_clean")
    customers = dlt.read("silver_customers_scd2").filter(col("__end_at").isNull())

    enriched = calls.join(
        customers.select("customer_id", "city"),
        on="customer_id",
        how="left",
    )

    # -- Call-level collapse (from exploded segment rows) -------------------
    call_level = (
        enriched
        .groupBy("call_id", "city", to_date("event_ts").alias("date"))
        .agg(avg("sentiment_score").alias("call_sentiment_score"))
        .withColumn(
            "call_label",
            when(col("call_sentiment_score") > 0.1, "positive")
            .when(col("call_sentiment_score") < -0.1, "negative")
            .otherwise("neutral"),
        )
    )

    # -- City + date aggregation --------------------------------------------
    return (
        call_level
        .groupBy("city", "date")
        .agg(
            countDistinct("call_id").alias("total_calls"),
            spark_round(avg("call_sentiment_score"), 4).alias("avg_sentiment_score"),
            spark_sum(when(col("call_label") == "positive", 1).otherwise(0)).alias("positive_calls"),
            spark_sum(when(col("call_label") == "negative", 1).otherwise(0)).alias("negative_calls"),
            spark_sum(when(col("call_label") == "neutral", 1).otherwise(0)).alias("neutral_calls"),
        )
        .withColumn(
            "sentiment_ratio",
            spark_round(col("positive_calls") / col("total_calls").cast("double"), 4),
        )
        .orderBy("date", "city")
    )
