# Superstore Sales Analysis using SQL and Python

## Project Overview

This project analyzes the Superstore dataset using SQL, SQLite, Python, Pandas, and Matplotlib. The objective was to perform data exploration, data quality validation, sales analysis, profit analysis, customer analysis, and product analysis to generate meaningful business insights from transactional data.

The project combines SQL-based data analysis with Python-based visualization to demonstrate how raw business data can be transformed into actionable insights.

---

## Objectives

* Load and manage data using SQLite
* Explore dataset structure and contents
* Validate data quality and identify inconsistencies
* Analyze sales performance across regions, categories, and time periods
* Analyze profitability across products and regions
* Study customer purchasing behavior
* Evaluate product performance
* Generate business insights and recommendations
* Visualize important trends using charts and graphs

---

## Technologies Used

* Python
* Pandas
* SQLite
* SQL
* Matplotlib
* Jupyter Notebook

---

## Dataset Information

The Superstore dataset contains transactional sales records including:

* Orders
* Customers
* Products
* Categories and Sub-Categories
* Regions
* Sales
* Profit
* Discount
* Quantity

The dataset was imported into SQLite and queried using SQL for analysis.

---

## Project Structure

```text
week2_assignment/
│
├── Superstore Sales Analysis.ipynb
├── superstore_analysis.sql
├── Sample - Superstore.xlsx
├── superstore.db
├── karan_analysis.html
└── README.md
```

---

## Analysis Performed

### 1. Data Exploration

* Dataset preview
* Row count analysis
* Unique order analysis
* Schema inspection
* Region identification
* Category identification

### 2. Data Quality Checks

* Missing value detection
* Duplicate record validation
* Invalid discount checks
* Negative sales detection
* Negative profit analysis
* Missing customer information validation

### 3. Sales Analysis

* Total sales analysis
* Regional sales comparison
* Category sales analysis
* Segment sales analysis
* Top-selling products
* State-wise sales performance
* Yearly sales trends
* Monthly sales trends
* Highest-value orders

### 4. Profit Analysis

* Total profit calculation
* Regional profit comparison
* Category profit analysis
* Most profitable products
* Least profitable products

### 5. Customer Analysis

* Top customers by sales
* Top customers by profit
* Customer segmentation analysis
* Average sales per customer segment

### 6. Product Analysis

* Top-selling products
* Most profitable products
* Loss-making products
* Sales by sub-category
* Profit by sub-category

### 7. Business Insights

* Regional performance evaluation
* Yearly business growth analysis
* High-value customer identification
* Category performance comparison
* Strategic recommendations

---

## Key Findings

* Total sales exceeded 2.29 million.
* Total profit exceeded 286 thousand.
* The West region generated the highest sales and profit.
* Technology was the most profitable category.
* Consumer customers represented the largest customer segment.
* Several products generated consistent losses despite producing sales.
* Business performance showed steady growth over multiple years.

---

## Visualizations

The notebook includes multiple visualizations such as:

* Sales by Region
* Profit by Region
* Yearly Sales Trends
* Monthly Sales Trends
* Customer Analysis Charts
* Product Performance Charts
* Category Comparisons

---

## Conclusion

This project demonstrates how SQL and Python can be used together for business intelligence and data analysis. By combining SQL-based querying with Python-based visualization, meaningful insights were extracted from transactional sales data to support business decision-making.

The analysis successfully achieved all project objectives and provided valuable insights into sales performance, profitability, customer behavior, and product performance.
