"""
silver_calls_clean.py — Silver Layer: Cleaned & Enriched Call Events
=====================================================================
Reads from bronze_calls and applies all structural cleaning, enrichment,
and stream-processing safety patterns.

Transformations applied (in order)
-----------------------------------
1. epoch_timestamp → event_ts (TimestampType) for event-time processing.
2. withWatermark("event_ts", "30 minutes") — bounds memory for deduplication
   and aggregation while tolerating realistic late-arriving records.
3. dropDuplicates(["call_id", "event_ts"]) — idempotent deduplication on a
   composite key (call_id alone is insufficient because the same event can
   be re-delivered across micro-batches with a different ingestion_timestamp).
4. explode(transcript_segments) → one row per spoken segment.
5. Sentiment scoring via shared UDF — produces sentiment_score (float) and
   sentiment_label (positive / negative / neutral).
6. Flatten metadata struct → call_type, channel, region as top-level columns.
7. @dlt.expect_or_drop("non_negative_duration") → malformed records are
   silently quarantined (they never make it to Gold KPIs).

Data quality gates
------------------
  non_negative_duration  : duration_sec >= 0  → DROP row
  valid_event_ts         : event_ts IS NOT NULL → DROP row
  valid_call_id          : call_id IS NOT NULL  → DROP row

Upstream  : bronze_calls
Downstream: gold_* (all five KPI tables)
"""

import dlt
from pyspark.sql.functions import (
    col,
    explode,
    from_unixtime,
    to_timestamp,
)
from src.utils.sentiment import SENTIMENT_SCORE_UDF, SENTIMENT_LABEL_UDF


# ---------------------------------------------------------------------------
# Silver Calls — Streaming Table
# ---------------------------------------------------------------------------

@dlt.table(
    name="silver_calls_clean",
    comment=(
        "Cleaned, deduplicated, and sentiment-enriched call event records. "
        "One row per transcript segment. "
        "Watermarked on event_ts with a 30-minute tolerance for late data."
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
    """
    Full transformation pipeline for call event records.

    Why composite-key deduplication?
    ---------------------------------
    In Structured Streaming, micro-batch retries can re-deliver records.
    Using (call_id, event_ts) as the deduplication key ensures idempotency
    even when the same call_id is re-delivered at a different ingestion time.

    Why withWatermark before dropDuplicates?
    -----------------------------------------
    Spark Structured Streaming requires a watermark to be set BEFORE
    stateful operations (dropDuplicates). The 30-min watermark means:
    'wait up to 30 minutes for out-of-order events, then purge state.'
    """
    bronze = dlt.read_stream("bronze_calls")

    # ── Step 1: Convert epoch to event_ts ──────────────────────────────────
    with_ts = bronze.withColumn(
        "event_ts",
        to_timestamp(from_unixtime(col("epoch_timestamp")))
    )

    # ── Step 2: Watermark (must precede stateful ops) ──────────────────────
    watermarked = with_ts.withWatermark("event_ts", "30 minutes")

    # ── Step 3: Deduplicate on composite key ───────────────────────────────
    deduped = watermarked.dropDuplicates(["call_id", "event_ts"])

    # ── Step 4: Flatten metadata struct ────────────────────────────────────
    flattened = (
        deduped
        .withColumn("call_type",  col("metadata.call_type"))
        .withColumn("channel",    col("metadata.channel"))
        .withColumn("region",     col("metadata.region"))
        .drop("metadata")
    )

    # ── Step 5: Explode transcript_segments ────────────────────────────────
    # Each call becomes N rows (one per spoken turn).
    # The call-level columns (call_id, customer_id, sentiment_*) repeat on
    # each row so Gold aggregations can GROUP BY call_id and take MAX/FIRST.
    exploded = (
        flattened
        .withColumn("segment", explode(col("transcript_segments")))
        .withColumn("speaker",   col("segment.speaker"))
        .withColumn("text",      col("segment.text"))
        .withColumn("start_sec", col("segment.start_sec"))
        .drop("transcript_segments", "segment")
    )

    # ── Step 6: Sentiment scoring ───────────────────────────────────────────
    # Applied to the full concatenated text of each spoken segment.
    # Gold tables aggregate sentiment_score at call level (avg or first).
    enriched = (
        exploded
        .withColumn("sentiment_score", SENTIMENT_SCORE_UDF(col("text")))
        .withColumn("sentiment_label", SENTIMENT_LABEL_UDF(col("sentiment_score")))
    )

    # ── Step 7: Drop internal debug markers from generator ─────────────────
    final_cols = [c for c in enriched.columns if c != "_injection_type"]
    return enriched.select(final_cols)
