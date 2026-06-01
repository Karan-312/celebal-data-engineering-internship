# ShopEase SQL Database Assignment

## Overview

This project is a SQL Server database implementation for the **ShopEase E-Commerce Database Assignment**. The objective of this assignment is to demonstrate understanding of database design, SQL querying, constraints, indexing, joins, aggregations, transactions, and ACID properties.

The database was created and tested using **Microsoft SQL Server**.

---

## Database Schema

The database consists of four related tables:

### Customers

Stores customer information including:

* Customer ID
* Name
* Email
* City
* State
* Join Date
* Premium Membership Status

### Products

Stores product details including:

* Product ID
* Product Name
* Category
* Brand
* Unit Price
* Stock Quantity

### Orders

Stores order information including:

* Order ID
* Customer ID
* Order Date
* Status
* Total Amount

### Order Items

Stores product-level order details including:

* Item ID
* Order ID
* Product ID
* Quantity
* Unit Price
* Discount Percentage

---

## Technologies Used

* Microsoft SQL Server
* SQL Server Management Studio (SSMS)
* SQL (DDL, DML, DQL)

---

## Assignment Sections

### Section A – SQL Basics

Topics covered:

* SELECT statements
* DISTINCT values
* Primary Keys
* Constraints
* Data validation

### Section B – Filtering & Optimization

Topics covered:

* WHERE clause
* Date filtering
* Indexes
* Query optimization
* SARGable queries

### Section C – Aggregation

Topics covered:

* COUNT()
* SUM()
* AVG()
* MIN()
* MAX()
* GROUP BY
* HAVING
* ORDER BY

### Section D – Joins & Relationships

Topics covered:

* INNER JOIN
* LEFT JOIN
* Multi-table joins
* Foreign Keys
* Referential Integrity

### Section E – Advanced Concepts

Topics covered:

* CASE expressions
* Conditional aggregation
* ACID properties
* Transaction management
* COMMIT and ROLLBACK

---

## Key Concepts Demonstrated

* Database normalization
* Primary and Foreign Key relationships
* Data integrity through constraints
* Query optimization using indexes
* Aggregation and analytical queries
* Multi-table joins
* Transaction handling
* ACID compliance

---

## Transaction Implementation

The assignment includes a transaction that:

1. Inserts a new order
2. Inserts related order items
3. Updates product inventory
4. Uses TRY...CATCH blocks
5. Performs COMMIT on success
6. Performs ROLLBACK on failure

This ensures atomic and reliable database operations.

---

## Learning Outcomes

Through this assignment, the following skills were practiced:

* Designing relational databases
* Writing SQL queries for business requirements
* Using aggregate functions for reporting
* Implementing joins across related tables
* Understanding database indexing
* Maintaining referential integrity
* Managing transactions safely
* Applying ACID principles

---
SQL Database Assignment – Week 2
