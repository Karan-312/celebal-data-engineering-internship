# Week 5 Data Engineering Assignment: PySpark Fundamentals
**Celebal Technologies Summer Internship**
**Author:** Karan Rudrawal

## 📌 Projfect Overview
This repository contains the completion of the Week 5 Data Engineering assignment. The primary objective is to demonstrate proficiency in Apache Spark fundamentals, focusing on data cleaning, transformation, and aggregation using PySpark DataFrames. 

Due to the absence of a provided source dataset, this project includes a custom Python script to programmatically generate a robust, synthetic dataset designed specifically to test edge cases (e.g., null values, empty strings, duplicates).

## 🛠️ Technologies Used
* **Apache Spark / PySpark:** Core data processing engine.
* **Python 3:** Scripting and synthetic data generation.
* **Jupyter Notebook:** Interactive development environment.
* **VS Code:** Primary IDE.

## 🗄️ Dataset Generation
The source data (`spark_large_dataset.csv`) is synthetically generated via `Dataset_Generator_Script.ipynb`. It simulates an e-commerce/retail transaction log with the following schema:
* `user_id` (String)
* `transaction_date` (Date string)
* `region` (String: North, South, East, West)
* `product_category` (String)
* `sale_amount` (Double)
* `status` (String - contains injected `null` values)
* `city` (String)
* `age` (Integer)
* `subscription` (String)
* `raw_timestamp` (String format of DateTime)
* `email` (String - contains injected `null` values)
* `username` (String - contains injected empty strings)
* `price` (Double - contains injected `null` values)
* `store_id` (String)

## 🚀 Key Operations & Transformations Performed
This assignment addresses 15 specific questions, categorized into theoretical concepts and practical PySpark implementations:

### Theoretical Concepts
* **Spark vs. MapReduce:** Advantages of In-Memory Processing over disk-reliant architectures.
* **Wide vs. Narrow Transformations:** Understanding the "Shuffle" process during grouping operations.
* **DataFrame Immutability:** How Spark handles state changes during data cleaning.
* **Schema Inference Risks:** Pitfalls of `inferSchema=True` with messy data types.

### Practical PySpark Implementations
1. **Data Cleaning:** 
   * Removing duplicate rows across subsets of columns (`dropDuplicates`).
   * Handling missing data (`na.drop()` vs `na.fill()`).
   * Filtering invalid records (null emails, empty usernames).
2. **Transformations:**
   * Schema manipulation and type casting (`cast(TimestampType())`).
   * Renaming columns dynamically (`withColumnRenamed`, `alias`).
3. **Aggregations & Filtering:**
   * Conditional filtering using `.filter()` and `col().between()`.
   * Grouping data by dimensions (e.g., `store_id`, `city`, `product_category`).
   * Computing multi-statistic aggregations (`min`, `max`, `avg`, `sum`).

## ⚙️ How to Run Locally

1. **Prerequisites:** Ensure you have Python 3.x, Apache Spark, and a compatible Java JDK (e.g., Java 17) installed and configured in your system environment variables.
2. **Install Dependencies:**
```bash
   pip install pyspark jupyter