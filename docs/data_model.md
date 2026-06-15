# Data Model (Star Schema)

The Gold layer implements a Star Schema optimized for analytical performance and clarity.

## Dimension Tables

### dim_customers
*   `customer_key`: (PK) Hashed email.
*   `first_name`, `last_name`: Personal details.
*   `city`, `country`: Geographic details.

### dim_products
*   `product_key`: (PK) Original product ID.
*   `name`: Product name.
*   `category`: Electronics, Accessories, etc.
*   `brand`: Brand name.

### dim_dates
*   Generated date dimension with `year`, `month`, `day`, `quarter`, `is_weekend`.

## Fact Tables

### fact_orders
*   `order_id`: (PK) Original order ID.
*   `customer_key`: (FK) Reference to `dim_customers`.
*   `order_date`: (FK) Reference to `dim_dates`.
*   `total_amount`: Order total revenue.
*   `order_status`: completed, pending, etc.
*   `payment_method`: credit_card, paypal, etc.
*   `paid_amount`: Actual amount paid.

## Analytical Marts

### mart_sales_daily
*   `sale_date`: Aggregation key.
*   `total_orders`: Count of orders per day.
*   `revenue`: Sum of revenue per day.
*   `avg_order_value`: Mean revenue per order.
