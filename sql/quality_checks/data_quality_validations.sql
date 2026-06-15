-- Data quality checks

-- 1. Check for nulls in critical columns (Silver/Gold)
SELECT 'orders_null_customer' as check_name, COUNT(*) as failed_records
FROM silver_orders WHERE customer_id IS NULL
UNION ALL
SELECT 'customers_null_email', COUNT(*)
FROM silver_customers WHERE email IS NULL;

-- 2. Check for duplicate records (Silver)
SELECT 'duplicate_customers' as check_name, COUNT(*)
FROM (
    SELECT email, COUNT(*)
    FROM silver_customers
    GROUP BY email
    HAVING COUNT(*) > 1
) t;

-- 3. Referential integrity check
SELECT 'orphan_order_items' as check_name, COUNT(*)
FROM silver_order_items oi
LEFT JOIN silver_orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- 4. Negative values check
SELECT 'negative_order_amounts' as check_name, COUNT(*)
FROM silver_orders WHERE total_amount < 0;
