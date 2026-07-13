"""
gold_subscription_sentiment.py — Gold KPI: Subscription-based Sentiment
=========================================================================
Business Question: Are Premium customers happier than Basic customers?

Grain       : 1 row per subscription_type per calendar day
Refresh     : Materialized View

Columns
-------
  subscription_type   string  — 'Basic' or 'Premium'
  date                date    — calendar date
  total_calls         long    — distinct calls for this tier on this date
  avg_sentiment_score double  — mean sentiment (higher = more positive)
  positive_calls      long    — positive-sentiment call count
  negative_calls      long    — negative-sentiment call count
  neutral_calls       long    — neutral-sentiment call count
  sentiment_ratio     double  — positive_calls / total_calls

Design note
-----------
  subscription_type is sourced from silver_customers_scd2 (__end_at IS NULL)
  — the customer's CURRENT plan. If a customer upgraded from Basic to Premium,
  all their future calls are counted under Premium.

  This is the correct business behavior for plan-satisfaction reporting:
  you want to know what plan the customer is on NOW, not what they were on
  when they called. For historical plan-level analysis, use a point-in-time
  SCD2 join.

Upstream  : silver_calls_clean, silver_customers_scd2
"""

import dlt
from pyspark.sql.functions import (
    avg, col, countDistinct, round as spark_round,
    sum as spark_sum, to_date, when,
)


@dlt.table(
    name="gold_subscription_sentiment",
    comment=(
        "Subscription-tier sentiment KPI. "
        "Segments sentiment by subscription type to uncover plan-specific satisfaction gaps."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_subscription_sentiment():
    calls = dlt.read("silver_calls_clean")
    customers = dlt.read("silver_customers_scd2").filter(col("__end_at").isNull())

    enriched = calls.join(
        customers.select("customer_id", "subscription_type"),
        on="customer_id",
        how="left",
    )

    # -- Call-level collapse ------------------------------------------------
    call_level = (
        enriched
        .groupBy("call_id", "subscription_type", to_date("event_ts").alias("date"))
        .agg(avg("sentiment_score").alias("call_sentiment_score"))
        .withColumn(
            "call_label",
            when(col("call_sentiment_score") > 0.1, "positive")
            .when(col("call_sentiment_score") < -0.1, "negative")
            .otherwise("neutral"),
        )
    )

    # -- Tier + date aggregation --------------------------------------------
    return (
        call_level
        .groupBy("subscription_type", "date")
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
        .orderBy("date", "subscription_type")
    )
