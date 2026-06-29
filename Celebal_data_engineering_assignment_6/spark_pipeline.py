"""
Spark Data Engineering Project
Employee Analytics Pipeline
"""

import os
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, LongType
)


# ─────────────────────────────────────────────
# 1. SparkSession — entry point to everything
# ─────────────────────────────────────────────
# The Driver lives here. It communicates with the Cluster Manager
# (local[2] in dev, YARN/K8s in prod) which allocates Executors.
# Each Executor runs tasks on partitions of your data in parallel.

spark = SparkSession.builder \
    .appName("EmployeeDataPipeline") \
    .master("local[2]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print(f"  App      : {spark.sparkContext.appName}")
print(f"  Master   : {spark.sparkContext.master}")
print(f"  Version  : {spark.version}")
print("=" * 60)


# ─────────────────────────────────────────────
# 2. Schema Definition
# ─────────────────────────────────────────────
# Always define schema explicitly on large datasets.
# Avoids the full-file scan that inferSchema triggers,
# and gives you type safety from the start.

employee_schema = StructType([
    StructField("employee_id", StringType(),  False),
    StructField("name",        StringType(),  True),
    StructField("department",  StringType(),  True),
    StructField("salary",      DoubleType(),  True),
    StructField("city",        StringType(),  True),
    StructField("join_date",   StringType(),  True),
    StructField("years_exp",   IntegerType(), True),
    StructField("status",      StringType(),  True),
])


# ─────────────────────────────────────────────
# 3. Read CSV
# ─────────────────────────────────────────────
# This is LAZY — no data moves yet. Spark just records what
# you want to do and builds a logical plan (DAG).

raw_df = spark.read \
    .schema(employee_schema) \
    .option("header",    "true") \
    .option("nullValue", "") \
    .csv("data/employees.csv")

print("\n── Schema ──────────────────────────────────")
raw_df.printSchema()

# .count() is an ACTION — this is where Spark actually executes the DAG
print(f"Total records: {raw_df.count()}")

print("\nSample rows (raw):")
raw_df.show(5, truncate=False)


# ─────────────────────────────────────────────
# 4. Null Analysis
# ─────────────────────────────────────────────
# Cast bool → int and sum to get a per-column null count in one pass.

print("\n── Null Count per Column ───────────────────")
null_report = raw_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in raw_df.columns
])
null_report.show()


# ─────────────────────────────────────────────
# 5. Filter — Narrow Transformation
# ─────────────────────────────────────────────
# Narrow = each input partition maps to at most one output partition.
# No shuffle. Catalyst Optimizer pushes these filters down to the scan.

active_df = raw_df.filter(
    (F.col("status") == "Active") &
    F.col("salary").isNotNull()
)

print(f"\nActive employees with valid salary: {active_df.count()}")


# ─────────────────────────────────────────────
# 6. Transformations — still lazy
# ─────────────────────────────────────────────

transformed_df = active_df \
    .withColumnRenamed("name", "full_name") \
    .withColumn("salary",       F.col("salary").cast(LongType())) \
    .withColumn("join_date",    F.to_date(F.col("join_date"), "yyyy-MM-dd")) \
    .withColumn("salary_band",
        F.when(F.col("salary") < 60_000,  "Junior")
         .when(F.col("salary") < 100_000, "Mid")
         .when(F.col("salary") < 140_000, "Senior")
         .otherwise("Lead")
    ) \
    .withColumn("annual_salary", F.col("salary") * 12) \
    .withColumn("tenure_years",
        F.round(
            F.datediff(F.current_date(), F.col("join_date")) / 365.0,
            1
        )
    ) \
    .select(
        "employee_id", "full_name", "department",
        "salary", "annual_salary", "salary_band",
        "city", "join_date", "tenure_years", "years_exp"
    )

print("\n── Transformed Sample ──────────────────────")
transformed_df.show(8, truncate=False)


# ─────────────────────────────────────────────
# 7. Aggregations — Wide Transformation
# ─────────────────────────────────────────────
# groupBy causes a SHUFFLE. Spark redistributes rows across
# partitions so that all rows with the same key land together.
# This is the most expensive operation — minimize where possible.

print("\n── Department Stats (shuffle happens here) ─")
dept_stats = transformed_df.groupBy("department").agg(
    F.count("*")                   .alias("headcount"),
    F.round(F.avg("salary"), 2)    .alias("avg_salary"),
    F.min("salary")                .alias("min_salary"),
    F.max("salary")                .alias("max_salary"),
    F.round(F.avg("tenure_years"), 1).alias("avg_tenure"),
    F.round(F.avg("years_exp"), 1) .alias("avg_exp")
).orderBy("avg_salary", ascending=False)

dept_stats.show(truncate=False)

print("\n── Salary Band Distribution ────────────────")
transformed_df.groupBy("salary_band").agg(
    F.count("*")               .alias("count"),
    F.round(F.avg("salary"), 0).alias("avg_salary")
).orderBy("avg_salary").show()

print("\n── City-wise Headcount ─────────────────────")
transformed_df.groupBy("city").agg(
    F.count("*")                  .alias("employees"),
    F.round(F.avg("salary"), 2)   .alias("avg_salary")
).orderBy("employees", ascending=False).show()

print("\n── Top 10 Earners ──────────────────────────")
transformed_df \
    .select("employee_id", "full_name", "department", "salary", "salary_band", "city") \
    .orderBy(F.col("salary").desc()) \
    .limit(10) \
    .show(truncate=False)


# ─────────────────────────────────────────────
# 8. Write — Parquet (columnar, compressed)
# ─────────────────────────────────────────────
# Parquet stores data by column, not row.
# Benefit: if you only query "salary", Spark reads only that column's bytes.
# partitionBy("department") creates subdirectories like department=Engineering/
# This lets Spark skip entire partitions when a filter matches — Predicate Pushdown.

print("\n── Writing Parquet (partitioned by department) ─")
transformed_df.write \
    .mode("overwrite") \
    .partitionBy("department") \
    .parquet("output/employees_parquet")
print("Done.")

print("\n── Writing dept summary as CSV ─────────────")
dept_stats.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("output/dept_summary_csv")
print("Done.")


# ─────────────────────────────────────────────
# 9. Read back Parquet — Predicate Pushdown
# ─────────────────────────────────────────────
# Spark reads ONLY the department=Engineering/ folder.
# It never opens the other five partitions — massive I/O saving at scale.

print("\n── Reading Parquet with Partition Filter ───")
eng_df = spark.read.parquet("output/employees_parquet") \
    .filter(F.col("department") == "Engineering")

print(f"Engineering records: {eng_df.count()}")
eng_df.show(5, truncate=False)

print("\n── Physical Plan (see PartitionFilters line) ─")
eng_df.explain(mode="simple")


# ─────────────────────────────────────────────
# 10. DAG / Lineage of the main pipeline
# ─────────────────────────────────────────────
# The Optimized Logical Plan shows how Catalyst rewrote your query:
#   - Collapsed multiple Projects into one
#   - Pushed the filter all the way down to the FileScan
# The Physical Plan shows what actually runs on Executors.

print("\n── DAG Lineage — transformed_df ────────────")
transformed_df.explain(mode="extended")


# ─────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────
spark.stop()
print("\n" + "=" * 60)
print("  Pipeline complete.")
print("  Output → output/employees_parquet/")
print("           output/dept_summary_csv/")
print("=" * 60)
