/*=========================================================
  WEEK 3 - SUBQUERIES AND CTEs
  Dataset: Superstore
  Author: Karan Rudrawal

  Objective:
  Practice Subqueries and Common Table Expressions (CTEs)
  to answer business questions using sales data.
=========================================================*/

/*=========================================================
  Q1. Find all orders where sales are greater than
      the average sales.
=========================================================*/

SELECT *
FROM orders
WHERE Sales > (
    SELECT AVG(Sales)
    FROM orders
);

/*
Insight:
- Returned 3,155 orders with sales above the average sales value.
- These orders represent the higher-value transactions in the dataset.
- Several orders exceeded $1,000 in sales, indicating the presence of high-ticket purchases.
- Useful for identifying revenue-driving transactions.
*/

/*=========================================================
  Q2. Find the highest sales order for each customer.
=========================================================*/

SELECT *
FROM orders o
WHERE Sales = (
    SELECT MAX(Sales)
    FROM orders
    WHERE Customer_ID = o.Customer_ID
);

/*
Insight:
- Identifies the highest-value order placed by each customer.
- Customer SM-20320 recorded the largest individual order worth $22,638.48.
- Customer TC-20980 recorded the second-largest order worth $17,499.95.
- Helps identify customers responsible for major revenue spikes.
*/

/*=========================================================
  Q3. Calculate total sales for each customer.
      (Using CTE)
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
ORDER BY Total_Sales DESC;

/*
Insight:
Top Customers:
1. Sean Miller      - 25,043.07
2. Tamara Chand     - 19,052.22
3. Raymond Buch     - 15,117.35

- Customer sales vary significantly.
- A small number of customers contribute
  a large portion of total revenue.
*/
/*=========================================================
  Q4. Find customers whose total sales are
      above average.
      (CTE + Subquery)
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
- Returned customers whose total sales exceed the average customer sales.
- Sean Miller generated the highest total sales ($25,043.07).
- Tamara Chand and Raymond Buch also significantly exceeded the average.
- These customers form the most valuable customer segment.
*/

/*=========================================================
  Q5. Rank all customers based on total sales.
      (Window Function)
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

SELECT
    Customer_ID,
    Customer_Name,
    Total_Sales,
    RANK() OVER (
        ORDER BY Total_Sales DESC
    ) AS Sales_Rank
FROM CustomerSales
ORDER BY Sales_Rank;

/*
Insight:
- Sean Miller achieved Rank 1 with total sales of $25,043.07.
- Tamara Chand achieved Rank 2 with total sales of $19,052.22.
- Raymond Buch achieved Rank 3 with total sales of $15,117.35.
- Ranking helps identify the most profitable customers.
*/

/*=========================================================
  Q6. Assign row numbers to each order within a customer.
      (Window Function + PARTITION BY)
=========================================================*/

SELECT
    Customer_ID,
    Order_ID,
    Order_Date,
    Sales,

    ROW_NUMBER() OVER (
        PARTITION BY Customer_ID
        ORDER BY Order_Date
    ) AS Order_Number

FROM orders
ORDER BY Customer_ID, Order_Number;
/*
Insight:
- Orders are numbered separately for each customer.
- The numbering restarts from 1 for every customer.
- Useful for tracking purchase sequence and customer activity.
*/


/*=========================================================
  Q7. Display top 3 customers based on total sales.
      (Window Function)
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
),

RankedCustomers AS (
    SELECT
        Customer_ID,
        Customer_Name,
        Total_Sales,
        RANK() OVER (
            ORDER BY Total_Sales DESC
        ) AS Sales_Rank
    FROM CustomerSales
)

SELECT *
FROM RankedCustomers
WHERE Sales_Rank <= 3
ORDER BY Sales_Rank;
    
/*
Insight:
Top 3 Customers:
1. Sean Miller      - 25,043.07
2. Tamara Chand     - 19,052.22
3. Raymond Buch     - 15,117.35

- These customers generated the highest revenue.
- They represent valuable customers for retention
  and loyalty programs.
*/

/*=========================================================
  Q8). FINAL COMBINED QUERY

  Concepts Used:
  - JOIN
  - CTE
  - Window Function (RANK)

  Output:
  - Customer Name
  - Total Sales
  - Sales Rank
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

SELECT
    Customer_Name,
    Total_Sales,
    RANK() OVER (
        ORDER BY Total_Sales DESC
    ) AS Sales_Rank
FROM CustomerSales
ORDER BY Sales_Rank;
/*
Insight:
- Sean Miller ranked #1 with total sales of 25,043.07.
- Tamara Chand ranked #2 with total sales of 19,052.22.
- Raymond Buch ranked #3 with total sales of 15,117.35.
- This query combines JOIN, CTE, and Window Functions into a single business report.
- Useful for customer segmentation and revenue analysis.
*/



/*=========================================================
  ADDITIONAL INSIGHTS (EXTRA ANALYSIS)
=========================================================*/

/*=========================================================
  Extra Analysis 1
  Total Sales by Region
=========================================================*/

SELECT
    c.Region,
    SUM(o.Sales) AS Total_Sales
FROM orders o
JOIN customers c
    ON o.Customer_ID = c.Customer_ID
GROUP BY c.Region
ORDER BY Total_Sales DESC;

/*
Insight:
- West region generated the highest sales with 2,104,438.72.
- South region followed with 1,385,717.71 in sales.
- East region contributed 526,776.75.
-- The West region appears to be the strongest market for the business.
- Central region recorded unusually low sales and should be interpreted
  with caution, as customer distribution after normalization may have
  affected regional totals.
- The West region appears to be the strongest market for the business.
*/

/*=========================================================
  Extra Analysis 2
  Top 10 Products by Revenue
=========================================================*/

SELECT TOP 10
    p.Product_Name,
    SUM(o.Sales) AS Total_Sales
FROM orders o
JOIN products p
    ON o.Product_ID = p.Product_ID
GROUP BY p.Product_Name
ORDER BY Total_Sales DESC;

/*
Insight:
- Canon imageCLASS 2200 Advanced Copier generated the highest revenue (61,599.83),
  more than double the revenue of the second-ranked product.
- Fellowes PB500 Electric Punch Plastic Comb Binding Machine generated 27,453.38 in sales.
- Cisco TelePresence System EX90 Videoconferencing generated 22,638.48 in sales.
- The top-performing products are primarily office equipment and technology-related products.
- Revenue is concentrated among a small number of high-value products,
  indicating their strong contribution to overall business performance.
*/
/*=========================================================
  Extra Analysis 3
  Average Order Value
=========================================================*/

SELECT
    AVG(Sales) AS Average_Order_Value
FROM orders;

/*
Insight:
- The average order value is approximately 229.86.
- Orders above this value can be considered high-value transactions.
- This benchmark can be used to evaluate customer purchasing behavior.
*/
/*=========================================================
  Extra Analysis 4
  Customers With Most Orders
=========================================================*/

SELECT TOP 10
    c.Customer_Name,
    COUNT(*) AS Order_Count
FROM orders o
JOIN customers c
    ON o.Customer_ID = c.Customer_ID
GROUP BY c.Customer_Name
ORDER BY Order_Count DESC;

/*
Insight:
- William Brown placed the highest number of orders (37).
- John Lee, Matt Abelman, and Paul Prost each placed 34 orders.
- Frequent purchases indicate strong customer engagement.
- These customers are strong candidates for retention and loyalty programs.
*/
/*=========================================================
  Extra Analysis 5
  Most Profitable Customers
=========================================================*/

SELECT TOP 10
    c.Customer_Name,
    SUM(o.Profit) AS Total_Profit
FROM orders o
JOIN customers c
    ON o.Customer_ID = c.Customer_ID
GROUP BY c.Customer_Name
ORDER BY Total_Profit DESC;
/*
Insight:
- Tamara Chand generated the highest profit (8,981.32).
- Raymond Buch and Sanjit Chand were the next most profitable customers.
- High sales and high profit do not always belong to the same customers.
- Profit analysis provides a better measure of customer value than sales alone.
*/