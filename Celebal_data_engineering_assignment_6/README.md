# Spark Data Engineering — Employee Analytics Pipeline

This assignment was about getting hands-on with Apache Spark — not just reading about how it works, but actually building something end-to-end. The goal was to understand the internals (Driver, Executors, DAG, lazy evaluation) and then use that understanding to write a real data pipeline.

---

## What I Built

A PySpark pipeline that reads raw employee data, cleans and transforms it, runs aggregations, and writes the output in both CSV and Parquet formats. The dataset has 1,000 employee records spread across 6 departments and 7 cities, with intentional nulls and mixed statuses to simulate real-world messiness.

---

## What I Learned

### Spark Architecture
Spark has three moving parts: the **Driver** (your program — it plans everything), the **Cluster Manager** (allocates resources), and the **Executors** (do the actual work on data partitions). In this project I ran it in `local[2]` mode, which spins up 2 executor threads on the same machine — great for development.

### Lazy Evaluation
This was one of the more interesting concepts to internalize. When you write `.filter()` or `.withColumn()`, nothing actually runs. Spark just records your intent and builds a logical plan called a DAG (Directed Acyclic Graph). The computation only kicks off when you call an **action** like `.count()` or `.show()`. This lets Spark's Catalyst Optimizer look at everything you want to do and figure out the most efficient way to do it before touching a single byte of data.

### Schema Handling
Instead of letting Spark infer the schema (which requires scanning the whole file), I defined it explicitly using `StructType`. This is faster and safer — you know exactly what types you're working with from the start.

### Null Handling
The dataset had 50 nulls in salary and 233 in status. I used a single-pass null scan (casting booleans to integers and summing) to get a per-column null count, then filtered out any rows where status wasn't "Active" or salary was missing before doing any transformations.

### Narrow vs Wide Transformations
- **Narrow** (`.filter()`, `.withColumn()`) — each partition handles itself, no data movement, fast.
- **Wide** (`.groupBy().agg()`) — data has to be shuffled across partitions so matching keys land together. This is the expensive part. I used it for department-level aggregations and salary band distributions, and kept `spark.sql.shuffle.partitions` low (4) for the small dataset.

### Column Transformations
Renamed columns, cast types (Double → Long for salary), parsed date strings into proper `DateType`, added derived columns like `annual_salary`, `salary_band` (Junior/Mid/Senior/Lead), and `tenure_years` calculated from join date to today.

### Predicate Pushdown
When I read back the Parquet file and filtered by `department = Engineering`, the physical plan showed Spark pushing that filter all the way down to the file scan — meaning it never even opened the other five department folders. You can see this clearly in `explain(mode="simple")` under `PartitionFilters`.

### File Formats — CSV vs Parquet
CSV is row-based and uncompressed. If you only need two columns out of ten, you still read everything. Parquet is columnar and compressed — it reads only the columns you ask for. Combined with `partitionBy("department")`, queries that filter on department skip entire folder trees on disk. At scale, this makes a dramatic difference.

---

## Pipeline Flow

```
employees.csv
     │
     ▼
Read with explicit schema
     │
     ▼
Null analysis → filter (Active + salary not null)
     │
     ▼
Transformations
  - rename, cast types, parse dates
  - add salary_band, annual_salary, tenure_years
     │
     ▼
Aggregations (groupBy department, city, salary band)
     │
     ▼
Write Parquet (partitioned by department)
Write CSV (dept summary)
     │
     ▼
Read back Parquet → verify Predicate Pushdown
```

---

## Output

| File | Description |
|---|---|
| `spark_pipeline.py` | Full PySpark pipeline |
| `employees.csv` | Sample dataset (1,000 records) |
| `output/employees_parquet/` | Partitioned Parquet output (by department) |
| `output/dept_summary_csv/` | Department-level aggregation summary |

---

## Quick Results

- **244 active employees** had valid salary out of 1,000
- **Sales** had the highest average salary (~₹1.18L/month), **Marketing** the lowest (~₹96K)
- **74 employees** fell in the Lead band (₹1.4L+) — most in any band
- Reading Engineering's Parquet partition skipped 5 out of 6 folders entirely

---

## How to Run

```bash
pip install pyspark

# generate the dataset first
python3 generate_data.py

# run the pipeline
python3 spark_pipeline.py
```

Requires Java 8+ (PySpark dependency).
