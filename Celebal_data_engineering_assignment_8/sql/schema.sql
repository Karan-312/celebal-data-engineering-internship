-- schema.sql
-- Clean, constrained schema for the e-commerce warehouse.
-- Loaded fresh every time clean_data.py runs, so it's fine that this drops
-- existing tables -- this is a staging/analytics DB, not production.

PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id   TEXT PRIMARY KEY,
    first_name    TEXT NOT NULL,
    last_name     TEXT NOT NULL,
    email         TEXT,
    city          TEXT,
    state         TEXT,
    signup_date   DATE NOT NULL
);

CREATE TABLE products (
    product_id    TEXT PRIMARY KEY,
    product_name  TEXT NOT NULL,
    category      TEXT NOT NULL,
    unit_price    REAL NOT NULL CHECK (unit_price >= 0),
    active        INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE orders (
    order_id       TEXT PRIMARY KEY,
    customer_id    TEXT NOT NULL,
    order_date     DATE NOT NULL,
    status         TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

CREATE TABLE order_items (
    order_item_id          TEXT PRIMARY KEY,
    order_id               TEXT NOT NULL,
    product_id             TEXT NOT NULL,
    quantity               INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_at_purchase REAL NOT NULL CHECK (unit_price_at_purchase >= 0),
    FOREIGN KEY (order_id) REFERENCES orders (order_id),
    FOREIGN KEY (product_id) REFERENCES products (product_id)
);

CREATE INDEX idx_orders_customer ON orders (customer_id);
CREATE INDEX idx_orders_date ON orders (order_date);
CREATE INDEX idx_order_items_order ON order_items (order_id);
CREATE INDEX idx_order_items_product ON order_items (product_id);
