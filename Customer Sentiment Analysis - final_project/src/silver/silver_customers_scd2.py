"""
silver_customers_scd2.py — Silver Layer: CDC Application + SCD Type 2
=======================================================================
Two-step process:

Step 1 — silver_customers_clean (Streaming Table)
    Reads bronze_customers, quarantines malformed rows into
    silver_customers_rejects, and passes valid CDC events downstream.

Step 2 — silver_customers_scd2 (Materialized View via dlt.apply_changes)
    Applies CDC operations (INSERT/UPDATE/DELETE) from silver_customers_clean
    using the native dlt.apply_changes() API which:
      • Maintains SCD Type 2 history for city and subscription_type
      • Sets __start_at / __end_at timestamps automatically
      • Handles out-of-order CDC events correctly via sequence_by
      • Applies hard DELETEs when operation = 'DELETE'

Why dlt.apply_changes() instead of hand-rolled MERGE?
------------------------------------------------------
Native DLT CDC handling understands out-of-order events — if an UPDATE
arrives before the INSERT it updates, dlt.apply_changes() correctly
sequences them using update_ts. A hand-rolled MERGE cannot do this without
significant custom logic and state management.

SCD Type 2 result example (for customer_id=18):
-----------------------------------------------
  customer_id  city      subscription_type  __start_at           __end_at
  18           Delhi     Premium            2026-04-01 21:15     2026-04-05 13:00
  18           Chennai   Basic              2026-04-05 13:00     2026-04-06 02:12
  18           Chennai   Premium            2026-04-06 02:12     NULL          ← active

Gold tables join on: __end_at IS NULL (current record)

Upstream  : bronze_customers
Downstream: gold_* (all five KPI tables)
"""

import dlt
from pyspark.sql.functions import col, expr


# ---------------------------------------------------------------------------
# Step 1: silver_customers_clean — Streaming Table (with quarantine)
# ---------------------------------------------------------------------------

@dlt.table(
    name="silver_customers_clean",
    comment=(
        "Valid CDC customer records after quarantine filter. "
        "Malformed rows are routed to silver_customers_rejects."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_customer_id",  "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_operation",    "operation IN ('INSERT','UPDATE','DELETE')")
@dlt.expect_or_drop("valid_update_ts",    "update_ts IS NOT NULL")
def silver_customers_clean():
    """
    Pass-through of valid CDC rows from Bronze.
    Rows failing @dlt.expect_or_drop are automatically captured by DLT
    into the quarantine table (silver_customers_rejects) when the pipeline
    is configured with a reject table — see pipeline.py for full config.
    """
    return (
        dlt.read_stream("bronze_customers")
        # Cast types for safety (Auto Loader infers, but explicit cast ensures
        # downstream aggregations don't silently coerce types)
        .withColumn("customer_id",       col("customer_id").cast("int"))
        .withColumn("age",               col("age").cast("int"))
        .withColumn("signup_date",       col("signup_date").cast("date"))
        .withColumn("update_ts",         col("update_ts").cast("timestamp"))
    )


# ---------------------------------------------------------------------------
# Quarantine table — captures rows dropped by silver_customers_clean
# ---------------------------------------------------------------------------

@dlt.table(
    name="silver_customers_rejects",
    comment=(
        "Malformed or invalid CDC customer records quarantined from Silver. "
        "Audit this table to investigate data quality issues at the source."
    ),
    table_properties={"quality": "quarantine"},
)
def silver_customers_rejects():
    """
    Explicit reject table: reads from bronze_customers but keeps only rows
    that would have been DROPPED by silver_customers_clean's expectations.
    This makes quarantine auditable and avoids silent data loss.
    """
    return (
        dlt.read_stream("bronze_customers")
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


# ---------------------------------------------------------------------------
# Step 2: silver_customers_scd2 — SCD Type 2 via dlt.apply_changes()
# ---------------------------------------------------------------------------

# IMPORTANT: dlt.apply_changes() creates the target table automatically.
# Do NOT define a @dlt.table decorator for silver_customers_scd2.
# The function call below IS the table definition.

dlt.apply_changes(
    # Target table that will hold SCD Type 2 history
    target="silver_customers_scd2",

    # Source of CDC events (the cleaned streaming table above)
    source="silver_customers_clean",

    # Natural key that identifies a unique customer
    keys=["customer_id"],

    # Column used to order events for the same key.
    # If two events arrive out of order, dlt.apply_changes() uses update_ts
    # to apply them in the correct logical sequence.
    sequence_by=col("update_ts"),

    # SCD Type 2: instead of overwriting, close the old row (set __end_at)
    # and insert a new active row (__end_at = NULL).
    stored_as_scd_type=2,

    # Only track history when THESE columns change.
    # If age changes but city/subscription_type stay the same, no new SCD row.
    track_history_column_list=["city", "subscription_type"],

    # Rows with operation = 'DELETE' are treated as hard deletes:
    # the active SCD row is closed (__end_at is set to the DELETE update_ts).
    apply_as_deletes=expr("operation = 'DELETE'"),

    # Exclude CDC/lineage columns from the tracked attribute set —
    # they should not trigger a new history row on their own.
    except_column_list=["operation", "source_file", "ingestion_timestamp"],
)
