"""
bronze_calls.py — Bronze Layer: Raw Call Events
================================================
Ingests the JSON call stream produced by generate_call_stream.py
via Databricks Auto Loader (cloudFiles format).

Design principles
-----------------
- ZERO business logic or transformation here.
- Append-only streaming table; every raw record is preserved forever.
- Auto Loader handles file tracking, exactly-once delivery, schema inference,
  and schema evolution automatically (addNewColumns mode).
- Operational metadata columns (source_file, ingestion_timestamp) are added
  here once and propagate through the entire pipeline for audit lineage.
- @dlt.expect rules are WARN-ONLY (not drop/fail) so no data is silently lost
  at the Bronze layer — failures are logged to the event log for review.

Upstream  : JSON files in the Volume landing zone
Downstream: silver_calls_clean
"""

import dlt
from pyspark.sql.functions import col, current_timestamp

# ---------------------------------------------------------------------------
# Configuration — values injected via databricks.yml pipeline configuration
# ---------------------------------------------------------------------------
CALLS_LANDING_PATH = spark.conf.get(
    "calls_landing_path",
    "/Volumes/celebal_catalog/sentiment_pipeline/landing/calls"
)


# ---------------------------------------------------------------------------
# Bronze Calls — Streaming Table
# ---------------------------------------------------------------------------

@dlt.table(
    name="bronze_calls",
    comment=(
        "Raw call event records ingested via Auto Loader. "
        "Append-only; zero transformation. "
        "Preserves full audit lineage via source_file and ingestion_timestamp."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect("valid_call_id",       "call_id IS NOT NULL")           # warn
@dlt.expect("valid_customer_id",   "customer_id IS NOT NULL")       # warn
@dlt.expect("valid_epoch",         "epoch_timestamp IS NOT NULL")   # warn
@dlt.expect("non_negative_duration", "duration_sec IS NULL OR duration_sec >= 0")  # warn
def bronze_calls():
    """
    Auto Loader streaming read from the JSON landing Volume.
    Schema is inferred on first run and evolved automatically when new
    columns appear in the source (addNewColumns mode).
    """
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",              "json")
             .option("cloudFiles.inferColumnTypes",    "true")
             # Automatically add new columns without restarting the pipeline
             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
             # Store schema files inside the pipeline catalog for durability
             .option("cloudFiles.schemaLocation",
                     f"/Volumes/celebal_catalog/sentiment_pipeline/landing/_schemas/calls")
             .load(CALLS_LANDING_PATH)
             # --- Operational metadata for lineage tracking ---
             .withColumn("source_file",          col("_metadata.file_path"))
             .withColumn("ingestion_timestamp",  current_timestamp())
    )
