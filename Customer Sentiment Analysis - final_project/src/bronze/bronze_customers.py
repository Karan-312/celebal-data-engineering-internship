"""
bronze_customers.py — Bronze Layer: CDC Customer Profiles
==========================================================
Ingests the customer CDC CSV batch (customer_cdc_data_final.csv) via
Auto Loader, treating it as a stream of change events.

Design principles
-----------------
- Full pass-through: every CDC row (INSERT / UPDATE / DELETE) is preserved.
- The `operation` and `update_ts` columns are the heart of CDC tracking —
  they must NEVER be dropped or transformed at this layer.
- @dlt.expect rules warn only (no drops) so malformed CDC rows can be
  audited downstream before being quarantined in Silver.
- source_file + ingestion_timestamp provide lineage just like bronze_calls.

CDC Schema (from customer_cdc_data_final.csv)
---------------------------------------------
  customer_id       int
  city              string
  subscription_type string
  age               int
  signup_date       date
  operation         string  (INSERT | UPDATE | DELETE)
  update_ts         timestamp

Upstream  : CSV files in the Volume landing zone
Downstream: silver_customers_clean → silver_customers_scd2
"""

import dlt
from pyspark.sql.functions import col, current_timestamp

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CUSTOMERS_LANDING_PATH = spark.conf.get(
    "customers_landing_path",
    "/Volumes/celebal_catalog/sentiment_pipeline/landing/customers"
)


# ---------------------------------------------------------------------------
# Bronze Customers — Streaming Table
# ---------------------------------------------------------------------------

@dlt.table(
    name="bronze_customers",
    comment=(
        "Raw CDC customer profile records from batch CSV. "
        "Full pass-through including operation and update_ts. "
        "Every INSERT, UPDATE, and DELETE event is preserved for Silver processing."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect("valid_customer_id",  "customer_id IS NOT NULL")         # warn
@dlt.expect("valid_operation",    "operation IN ('INSERT','UPDATE','DELETE')")  # warn
@dlt.expect("valid_update_ts",    "update_ts IS NOT NULL")           # warn
def bronze_customers():
    """
    Auto Loader streaming read from the CSV landing Volume.
    The header row is used to derive column names.
    Column types are inferred (customer_id → int, update_ts → timestamp, etc.).
    """
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",              "csv")
             .option("cloudFiles.inferColumnTypes",    "true")
             .option("header",                         "true")
             # CSV schema evolution: allow new columns without pipeline restart
             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
             .option("cloudFiles.schemaLocation",
                     f"/Volumes/celebal_catalog/sentiment_pipeline/landing/_schemas/customers")
             .load(CUSTOMERS_LANDING_PATH)
             # --- Operational metadata ---
             .withColumn("source_file",         col("_metadata.file_path"))
             .withColumn("ingestion_timestamp", current_timestamp())
    )
