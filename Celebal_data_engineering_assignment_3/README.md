# Customer Sales Insights – SQL Analysis Project

## Project Overview

This project analyzes the Superstore dataset using SQL to uncover insights related to customer behavior, sales performance, profitability, product revenue, and regional trends.

The project demonstrates practical usage of SQL for business intelligence and decision-making through a series of analytical queries and mini-project investigations.

---

## Dataset

* Dataset: Superstore
* Records Analyzed:

  * Customers: 793
  * Products: 1,862
  * Orders: 9,994

---

## SQL Concepts Used

* JOINs
* GROUP BY
* HAVING
* Aggregate Functions
* Subqueries
* Common Table Expressions (CTEs)
* Window Functions (RANK, ROW_NUMBER)
* Business Reporting Queries

---

## Key Business Insights

### Customer Analysis

* Sean Miller was the highest revenue-generating customer.
* A small group of customers contributed a significant share of total revenue.
* Several customers placed only a single order, highlighting retention opportunities.

### Sales Analysis

* Average Order Value was approximately $229.86.
* More than 3,000 orders exceeded the average sales value.
* High-value transactions were concentrated among a limited number of customers.

### Product Analysis

* Canon imageCLASS 2200 Advanced Copier generated the highest product revenue.
* Technology and office equipment products dominated sales performance.

### Regional Analysis

* The West region generated the highest overall sales.
* Regional sales performance varied significantly across markets.

### Profitability Analysis

* Tamara Chand generated the highest overall profit.
* High sales did not always correspond to high profitability.

---

## Project Structure

* Week 3 SQL Analysis (Q1–Q8)
* Additional Business Analyses (5 Reports)
* Customer Sales Mini Project (Q1–Q5)

Total Analyses Performed: 18

---

## Conclusion

This project demonstrates how SQL can transform raw transactional data into meaningful business insights. By combining data modeling, aggregation, subqueries, CTEs, and window functions, the project provides actionable insights into customer behavior, product performance, regional trends, and profitability.

## Files

- `Celebal_Assignment_3.ipynb` - Main Jupyter notebook containing all SQL analyses, query executions, results, and business insights.

- `table_creations.sql` - SQL script used to create the normalized database schema (Customers, Products, and Orders tables).

- `data_insertions.sql` - SQL script used to populate the normalized tables with data from the Superstore dataset.

- `questions.sql` - Week 3 assignment queries covering Subqueries, CTEs, and Window Functions (Q1–Q8).

- `mini_project.sql` - Additional customer sales analysis project containing business-focused SQL queries and insights.

- `making_table_new.sql` - Intermediate SQL script used during table creation, transformation, and database setup.

- `Sample - Superstore.csv` - Original dataset used for data normalization, analysis, and reporting.

- `README.md` - Project documentation, overview, SQL concepts used, and key business findings.