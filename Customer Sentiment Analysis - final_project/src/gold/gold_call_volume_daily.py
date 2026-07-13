"""
gold_call_volume_daily.py — Gold KPI: Daily Call Volume
=========================================================
Business Question: When are traffic peaks? Do we need more agents?

Grain       : 1 row per calendar day
Refresh     : Materialized View

Columns
-------
  date              date    — calendar date
  total_calls       long    — total distinct calls received that day
  avg_duration_sec  double  — average call duration in seconds
  max_duration_sec  long    — longest call of the day
  min_duration_sec  long    — shortest call of the day

Upstream  : silver_calls_clean
"""

import dlt
from pyspark.sql.functions import (
    avg, col, countDistinct, max as spark_max, min as spark_min,
    round as spark_round, to_date,
)


@dlt.table(
    name="gold_call_volume_daily",
    comment=(
        "Daily call volume KPI. "
        "Aggregates total calls per day to identify traffic peaks and operational patterns."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_call_volume_daily():
    calls = dlt.read("silver_calls_clean")

    # Collapse to call grain first (silver is segment-exploded)
    call_level = (
        calls
        .groupBy("call_id", to_date("event_ts").alias("date"))
        .agg(
            spark_max("duration_sec").alias("duration_sec"),  # same for all segments
        )
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
