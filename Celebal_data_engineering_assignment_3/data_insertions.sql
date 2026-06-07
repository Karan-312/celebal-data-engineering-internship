-- ==========================================
-- INSERT DATA INTO CUSTOMERS
-- ==========================================

INSERT INTO customers
SELECT DISTINCT
    Customer_ID,
    Customer_Name,
    Segment,
    Country,
    City,
    State,
    Postal_Code,
    Region
FROM superstore_raw;


-- ==========================================
-- INSERT DATA INTO PRODUCTS
-- ==========================================

INSERT INTO products
SELECT DISTINCT
    Product_ID,
    Product_Name,
    Category,
    Sub_Category
FROM superstore_raw;


-- ==========================================
-- INSERT DATA INTO ORDERS
-- ==========================================

INSERT INTO orders
SELECT
    Row_ID,
    Order_ID,
    Order_Date,
    Ship_Date,
    Ship_Mode,
    Customer_ID,
    Product_ID,
    Sales,
    Quantity,
    Discount,
    Profit
FROM superstore_raw;