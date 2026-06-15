# Data Model Documentation

## Source Systems

### PostgreSQL (Transactional)
- **customers**: `customer_id` (PK), `first_name`, `last_name`, `email`, `phone`, `address`, `city`, `country`.
- **orders**: `order_id` (PK), `customer_id` (FK), `order_date`, `status`, `total_amount`.
- **order_items**: `order_item_id` (PK), `order_id` (FK), `product_id`, `quantity`, `unit_price`.

### MySQL (Billing)
- **invoices**: `invoice_id` (PK), `order_id`, `invoice_date`, `due_date`, `amount`, `status`.
- **payments**: `payment_id` (PK), `invoice_id` (FK), `payment_date`, `amount`, `payment_method`, `transaction_id`.

### MongoDB (Catalog)
- **products**: `product_id`, `name`, `category`, `brand`, `price`, `specs`, `stock`.
- **user_activity**: `user_id`, `activity_type`, `product_id`, `timestamp`, `platform`.

### Cassandra (Events)
- **application_logs**: `log_id`, `service_name`, `log_level`, `message`, `log_timestamp`.
- **user_events**: `event_id`, `user_id`, `event_type`, `event_data`, `event_timestamp`.

## Data Lake Layers

### Silver Layer
- Tables are clean, deduplicated, and Parquet-formatted.
- Same schema as source but with added `processed_timestamp`.

### Gold Layer
- **gold_customer_orders**: Joins customers and orders for sales analysis.
- **gold_product_sales**: Aggregates sales by product, category, and brand.
