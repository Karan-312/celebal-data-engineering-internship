CREATE TABLE customers (
    Customer_ID   VARCHAR(20) PRIMARY KEY,
    Customer_Name VARCHAR(100),
    Segment       VARCHAR(50),
    Country       VARCHAR(50),
    City          VARCHAR(50),
    State         VARCHAR(50),
    Postal_Code   VARCHAR(20),
    Region        VARCHAR(50)

);
CREATE TABLE products (
    Product_ID   VARCHAR(50) PRIMARY KEY,
    Product_Name VARCHAR(255),
    Category     VARCHAR(50),
    Sub_Category VARCHAR(50)
); 
CREATE TABLE orders (
    Row_ID       INT PRIMARY KEY,
    Order_ID     VARCHAR(50),
    Order_Date   DATE,
    Ship_Date    DATE,
    Ship_Mode    VARCHAR(50),

    Customer_ID  VARCHAR(20),
    Product_ID   VARCHAR(50),

    Sales        DECIMAL(10,2),
    Quantity     INT,
    Discount     DECIMAL(5,2),
    Profit       DECIMAL(10,2),

    FOREIGN KEY (Customer_ID)
        REFERENCES customers(Customer_ID),

    FOREIGN KEY (Product_ID)
        REFERENCES products(Product_ID)
);
