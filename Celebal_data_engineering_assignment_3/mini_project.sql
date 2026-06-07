/*=========================================================
  MINI PROJECT
  Customer Sales Insights

  Dataset: Superstore
  Concepts Used:
  - JOIN
  - GROUP BY
  - HAVING
  - CTE
  - Subquery
  - Aggregate Functions
=========================================================*/
/*=========================================================
  MINI PROJECT - Q1
  Top 5 Customers by Total Sales
=========================================================*/

WITH CustomerSales AS (
    SELECT
        c.Customer_ID,
        c.Customer_Name,
        SUM(o.Sales) AS Total_Sales
    FROM orders o
    JOIN customers c
        ON o.Customer_ID = c.Customer_ID
    GROUP BY
        c.Customer_ID,
        c.Customer_Name
)

SELECT TOP 5 *
FROM CustomerSales
ORDER BY Total_Sales DESC;

/*
Insight:
- Sean Miller generated the highest total sales ($25,043.07).
- Tamara Chand ranked second with $19,052.22 in sales.
- Raymond Buch secured third place with $15,117.35.
- The top 5 customers each generated more than $14,000 in sales.
- These customers contribute significantly to overall revenue and should be prioritized for retention strategies.
*/
/*=========================================================
  MINI PROJECT - Q2
  Bottom 5 Customers by Total Sales
=========================================================*/

WITH CustomerSales AS (
    SELECT
        c.Customer_ID,
        c.Customer_Name,
        SUM(o.Sales) AS Total_Sales
    FROM orders o
    JOIN customers c
        ON o.Customer_ID = c.Customer_ID
    GROUP BY
        c.Customer_ID,
        c.Customer_Name
)

SELECT TOP 5 *
FROM CustomerSales
ORDER BY Total_Sales ASC;

/*
Insight:
- Thais Sisman generated the lowest sales ($4.83).
- Lela Donovan recorded only $5.30 in total sales.
- The bottom 5 customers all generated less than $25 in sales.
- These customers may represent one-time or inactive buyers.
- Targeted promotions could help increase engagement from this group.
*/
/*=========================================================
  MINI PROJECT - Q3
  Customers Who Made Only One Order
=========================================================*/

SELECT
    c.Customer_ID,
    c.Customer_Name,
    COUNT(o.Order_ID) AS Order_Count
FROM customers c
JOIN orders o
    ON c.Customer_ID = o.Customer_ID
GROUP BY
    c.Customer_ID,
    c.Customer_Name
HAVING COUNT(o.Order_ID) = 1;

/*
Insight:
- Several customers placed only one order in the dataset.
- Examples include Anthony O'Donnell, Carl Jackson, and Jocasta Rupert.
- One-time customers may indicate poor retention or recent acquisitions.
- These customers are ideal candidates for re-engagement campaigns.
*/
/*=========================================================
  MINI PROJECT - Q4
  Customers With Above Average Sales
=========================================================*/

WITH CustomerSales AS (
    SELECT
        c.Customer_ID,
        c.Customer_Name,
        SUM(o.Sales) AS Total_Sales
    FROM orders o
    JOIN customers c
        ON o.Customer_ID = c.Customer_ID
    GROUP BY
        c.Customer_ID,
        c.Customer_Name
)

SELECT *
FROM CustomerSales
WHERE Total_Sales > (
    SELECT AVG(Total_Sales)
    FROM CustomerSales
)
ORDER BY Total_Sales DESC;

/*
Insight:
- Customers above the average sales threshold represent the most valuable segment.
- Sean Miller leads this group with $25,043.07 in total sales.
- Many customers in this segment generated over $10,000 in revenue.
- Focusing marketing efforts on these customers can maximize business value.
*/
/*=========================================================
  MINI PROJECT - Q5
  Highest Order Value Per Customer
=========================================================*/

SELECT
    Customer_ID,
    MAX(Sales) AS Highest_Order_Value
FROM orders
GROUP BY Customer_ID
ORDER BY Highest_Order_Value DESC;

/*
Insight:
- Customer SM-20320 placed the highest single order worth $22,638.48.
- Customer TC-20980 recorded the second-highest order at $17,499.95.
- Customer RB-19360 recorded a maximum order value of $13,999.96.
- High-value orders are concentrated among a small group of customers.
- These customers represent important revenue opportunities for premium offerings.
*/