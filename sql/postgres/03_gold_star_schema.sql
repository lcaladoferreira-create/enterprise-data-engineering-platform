-- Gold Layer Star Schema DDL (PostgreSQL example)
-- These tables represent the final analytical structures

CREATE SCHEMA IF NOT EXISTS gold;

-- Dimension: Customers (Slowly Changing Dimension Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_customers (
    customer_key VARCHAR(64) PRIMARY KEY, -- Hashed PII email or UUID
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS gold.dim_products (
    product_key INT PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(100)
);

-- Fact: Orders
CREATE TABLE IF NOT EXISTS gold.fact_orders (
    order_id INT PRIMARY KEY,
    customer_key VARCHAR(64) REFERENCES gold.dim_customers(customer_key),
    order_date TIMESTAMP,
    total_amount DECIMAL(15, 2),
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    paid_amount DECIMAL(15, 2)
);

-- Aggregated Mart: Daily Sales Performance
CREATE TABLE IF NOT EXISTS gold.mart_sales_daily (
    sale_date DATE PRIMARY KEY,
    total_orders INT,
    revenue DECIMAL(18, 2),
    avg_order_value DECIMAL(15, 2)
);
