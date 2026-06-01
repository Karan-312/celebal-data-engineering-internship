USE ShopEase_DB;
GO

-- ==========================================
-- SECTION A - SQL BASICS
-- ==========================================

--------------------------------------------------
-- Q1. Write a query to display all columns and rows from the customer's table
--------------------------------------------------

SELECT *
FROM customers;

--------------------------------------------------
-- Q2. Retrieve only the first_name, last_name, and city of all customers
--------------------------------------------------

SELECT
    first_name,
    last_name,
    city
FROM customers;

--------------------------------------------------
-- Q3.  List all unique categories available in the products table.
--------------------------------------------------

SELECT DISTINCT category
FROM products;
--------------------------------------------------
-- Q4. Identify the Primary Key of each table in the schema. Explain why a Primary Key must be unique and NOT
--NULL
--------------------------------------------------

/*
customers   -> customer_id
products    -> product_id
orders      -> order_id
order_items -> item_id

Why PRIMARY KEY must be UNIQUE and NOT NULL?

- UNIQUE ensures every row can be identified individually.
- NOT NULL ensures every record has a valid identifier.
- Without these rules, relationships between tables and data integrity would break.
*/

--------------------------------------------------
-- Q5.What constraints are applied to the email column in the customers table? What would happen if you tried
-- to insert a duplicate email?
--------------------------------------------------

/*
The email column has:

1. NOT NULL constraint
2. UNIQUE constraint

Definition:
email VARCHAR(100) UNIQUE NOT NULL

If a duplicate email is inserted, SQL Server will throw
a UNIQUE KEY constraint violation error and reject the row.
*/

-- Example (will fail)
/*
INSERT INTO customers
VALUES
(
    109,
    'Test',
    'User',
    'aarav.s@email.com',
    'Mumbai',
    'Maharashtra',
    '2024-09-01',
    0
);
*/
--------------------------------------------------
-- Q6. Try inserting a product with unit_price = -50. What happens and which constraint prevents it? Write both
-- the INSERT statement and explain the error.
--------------------------------------------------

-- This statement should fail
/*
INSERT INTO products
VALUES
(
    209,
    'Test Product',
    'Electronics',
    'TestBrand',
    -50,
    100
);
*/
/*
Result:

SQL Server rejects the insert because unit_price
must be greater than 0.

Constraint responsible:

CHECK (unit_price > 0)

This CHECK constraint enforces valid positive pricing
for all products.
*/

-- ==========================================
-- SECTION B - FILTERING & OPTIMIZATION
-- ==========================================
--------------------------------------------------
-- Q7. Retrieve all orders with status = 'Delivered'
--------------------------------------------------

SELECT *
FROM orders
WHERE status = 'Delivered';

--------------------------------------------------
-- Q8. Find all products in the 'Electronics' category with a unit_price greater than ₹2000.
--------------------------------------------------

SELECT *
FROM products
WHERE category = 'Electronics'
  AND unit_price > 2000;

--------------------------------------------------
-- Q9.List all customers who joined in the year 2024 and belong to the state 'Maharashtra'.
--------------------------------------------------

SELECT *
FROM customers
WHERE state = 'Maharashtra'
  AND join_date >= '2024-01-01'
  AND join_date < '2025-01-01';

--------------------------------------------------
-- Q10. Find all orders placed between '2024-08-10' and '2024-08-25' (inclusive) that are NOT cancelled
--------------------------------------------------

SELECT *
FROM orders
WHERE order_date BETWEEN '2024-08-10' AND '2024-08-25'
  AND status <> 'Cancelled';

--------------------------------------------------
-- Q11. Explain what the index idx_orders_date does. How would it improve the performance of a query that
--filters orders by order_date? Write a sample query that would benefit from this index.
--------------------------------------------------

/*
idx_orders_date is an index created on the order_date column.

Purpose:
- Speeds up filtering, sorting, and searching operations
  involving order_date.
- Reduces the number of rows SQL Server must scan.

Without the index:
SQL Server may perform a full table scan.

With the index:
SQL Server can quickly locate matching dates using
the index structure.

Example query benefiting from this index:
*/

SELECT *
FROM orders
WHERE order_date BETWEEN '2024-08-01' AND '2024-08-31';

--------------------------------------------------
-- Q12. If you run: SELECT * FROM customers WHERE YEAR(join_date) = 2024; — would the index on join_date
--be used? Explain why or why not, and rewrite the query to be index-friendly (SARGable).
--------------------------------------------------

/*
Query:

SELECT *
FROM customers
WHERE YEAR(join_date) = 2024;

This query is NOT SARGable.

Reason:
Applying YEAR() to join_date forces SQL Server to
calculate YEAR() for every row before filtering.
This prevents efficient use of an index on join_date.

Index-friendly (SARGable) version:
*/

SELECT *
FROM customers
WHERE join_date >= '2024-01-01'
  AND join_date < '2025-01-01';

/*
This version allows SQL Server to seek directly
into the index instead of scanning all rows.
*/

-- ==========================================
-- SECTION C - AGGREGATION
-- ==========================================

--------------------------------------------------
-- Q13. Count the total number of orders in the orders table
--------------------------------------------------

SELECT COUNT(*) AS TotalOrders
FROM orders;

--------------------------------------------------
-- Q14. Find the total revenue (SUM of total_amount) from all 'Delivered' orders.
--------------------------------------------------

SELECT SUM(total_amount) AS TotalRevenue
FROM orders
WHERE status = 'Delivered';

--------------------------------------------------
-- Q15.  Calculate the average unit_price of products in each category.
--------------------------------------------------

SELECT
    category,
    AVG(unit_price) AS AveragePrice
FROM products
GROUP BY category;
--------------------------------------------------
-- Q16. For each order status, find the count of orders and the total revenue. Sort the result by total revenue in
--descending order.

--------------------------------------------------

SELECT
    status,
    COUNT(*) AS OrderCount,
    SUM(total_amount) AS TotalRevenue
FROM orders
GROUP BY status
ORDER BY TotalRevenue DESC;
--------------------------------------------------
-- Q17. Find the most expensive (MAX) and cheapest (MIN) product in each category.

--------------------------------------------------

SELECT
    category,
    MAX(unit_price) AS MostExpensive,
    MIN(unit_price) AS Cheapest
FROM products
GROUP BY category;
--------------------------------------------------
-- Q18.List all product categories where the average unit_price is greater than ₹2000. (Hint: Use HAVING
-- clause)
--------------------------------------------------

SELECT
    category,
    AVG(unit_price) AS AveragePrice
FROM products
GROUP BY category
HAVING AVG(unit_price) > 2000;

-- ==========================================
-- SECTION D - JOINS & RELATIONSHIPS
-- ==========================================

--------------------------------------------------
-- Q19. Write an INNER JOIN query to display each order along with the customer's first_name and last_name.
--Show: order_id, order_date, first_name, last_name, total_amount.
--------------------------------------------------

SELECT
    o.order_id,
    o.order_date,
    c.first_name,
    c.last_name,
    o.total_amount
FROM orders o
INNER JOIN customers c
ON o.customer_id = c.customer_id;

--------------------------------------------------
-- Q20.Using a LEFT JOIN, list ALL customers and their orders (if any). Customers with no orders should still
--appear with NULL values for order columns
--------------------------------------------------

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    o.order_id,
    o.order_date,
    o.status
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id;

--------------------------------------------------
-- Q21. Write a query using JOINs across three tables (orders → order_items → products) to show: order_id,
--product_name, quantity, unit_price, and discount_pct for each order item
--------------------------------------------------

SELECT
    o.order_id,
    p.product_name,
    oi.quantity,
    oi.unit_price,
    oi.discount_pct
FROM orders o
INNER JOIN order_items oi
    ON o.order_id = oi.order_id
INNER JOIN products p
    ON oi.product_id = p.product_id;

--------------------------------------------------
-- Q22.  Explain the difference between LEFT JOIN and RIGHT JOIN with an example from this schema. When
--would you use a FULL OUTER JOIN?
--------------------------------------------------

/*
LEFT JOIN:
Returns all records from the left table and
matching records from the right table.

Example:

SELECT *
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id;

All customers are returned even if they have no orders.


RIGHT JOIN:
Returns all records from the right table and
matching records from the left table.

Example:

SELECT *
FROM customers c
RIGHT JOIN orders o
ON c.customer_id = o.customer_id;

All orders are returned even if they have no matching customer.


FULL OUTER JOIN:
Returns all records from both tables.

Use when you want:
- customers without orders
- orders without customers

in a single result set.
*/


--------------------------------------------------
-- Q23.Identify all Foreign Key relationships in the schema. Explain what would happen if you tried to insert an
-- order with customer_id = 999 (which doesn't exist in customers).
--------------------------------------------------
/*
INSERT INTO orders
VALUES
(
    2000,
    999,
    '2024-09-01',
    'Pending',
    1000.00
);
*/
/*
Result:
SQL Server will reject the insert because
customer_id = 999 does not exist in customers.

The FOREIGN KEY constraint ensures that every
order references a valid customer.

Error:
The INSERT statement conflicted with the
FOREIGN KEY constraint.
*/

-- ==========================================
-- SECTION E - ADVANCED CONCEPTS
-- ==========================================

--------------------------------------------------
-- Q24. Query using CASE to classify products into price tiers:
 --• 'Budget' → unit_price < 1000
 --• 'Mid-Range' → unit_price BETWEEN 1000 AND 3000
 --• 'Premium' → unit_price > 3000
--Display: product_name, unit_price, price_tier
--------------------------------------------------

SELECT
    product_name,
    unit_price,
    CASE
        WHEN unit_price < 1000 THEN 'Budget'
        WHEN unit_price BETWEEN 1000 AND 3000 THEN 'Mid-Range'
        ELSE 'Premium'
    END AS price_tier
FROM products;
--------------------------------------------------
-- Q25. Using a CASE statement inside an aggregate function, count how many orders are 'Delivered' vs 'Not
--Delivered' (all other statuses). Display the result in a single row.

--------------------------------------------------

SELECT
    SUM(
        CASE
            WHEN status = 'Delivered' THEN 1
            ELSE 0
        END
    ) AS DeliveredOrders,

    SUM(
        CASE
            WHEN status <> 'Delivered' THEN 1
            ELSE 0
        END
    ) AS NotDeliveredOrders

FROM orders;
--------------------------------------------------
-- Q26. Explain each letter of ACID:
-- • A – Atomicity
-- • C – Consistency
-- • I – Isolation
-- • D – Durability
--Give a real-world example (e.g., bank transfer) showing why each property is important
--------------------------------------------------

/*
ACID is a set of properties that ensure database
transactions are processed reliably.

A - Atomicity
A transaction is treated as a single unit of work.
Either all operations succeed or all operations fail.

Example:
In a bank transfer of ₹1000 from Account A to Account B,
the money should be deducted from A and added to B.
If the addition to B fails, the deduction from A must
also be rolled back.

Why important:
Prevents partial transactions.


C - Consistency
A transaction must take the database from one valid
state to another valid state while maintaining all
constraints and rules.

Example:
A bank account balance cannot become invalid due to
a transaction that violates database rules.

Why important:
Maintains data integrity and accuracy.


I - Isolation
Multiple transactions running at the same time should
not interfere with each other.

Example:
If two customers purchase the last product in stock
simultaneously, the database should prevent incorrect
inventory updates.

Why important:
Avoids conflicts and inconsistent results.


D - Durability
Once a transaction is committed, the changes are
permanently stored and will survive system failures.

Example:
After a bank transfer is successfully completed,
the updated account balances remain saved even if
the server crashes immediately afterward.

Why important:
Ensures committed data is never lost.
*/
--------------------------------------------------
-- Q27. Write a SQL transaction that does the following atomically:
-- 1. Insert a new order (order_id=1011, customer_id=102, today's date, 'Pending', 1598.00)
-- 2. Insert two order items for that order
-- 3. Update the stock_qty of the purchased products
-- 4. If any step fails, ROLLBACK the entire transaction. Otherwise, COMMIT.
-- Write the complete BEGIN...COMMIT/ROLLBACK block.
--------------------------------------------------

BEGIN TRY

    BEGIN TRANSACTION;

--------------------------------------------------
-- Step 1: Insert new order
--------------------------------------------------

    INSERT INTO orders
    (
        order_id,
        customer_id,
        order_date,
        status,
        total_amount
    )
    VALUES
    (
        1011,
        102,
        GETDATE(),
        'Pending',
        1598.00
    );

--------------------------------------------------
-- Step 2: Insert order items
--------------------------------------------------

    INSERT INTO order_items
    (
        item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_pct
    )
    VALUES
    (
        5016,
        1011,
        202,
        1,
        799.00,
        0
    );

    INSERT INTO order_items
    (
        item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_pct
    )
    VALUES
    (
        5017,
        1011,
        208,
        1,
        799.00,
        0
    );

--------------------------------------------------
-- Step 3: Update stock quantities
--------------------------------------------------

    UPDATE products
    SET stock_qty = stock_qty - 1
    WHERE product_id = 202;

    UPDATE products
    SET stock_qty = stock_qty - 1
    WHERE product_id = 208;

--------------------------------------------------
-- Step 4: Commit transaction
--------------------------------------------------

    COMMIT TRANSACTION;

    PRINT 'Transaction completed successfully.';

END TRY

BEGIN CATCH

    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    PRINT 'Transaction failed. Changes rolled back.';

END CATCH;