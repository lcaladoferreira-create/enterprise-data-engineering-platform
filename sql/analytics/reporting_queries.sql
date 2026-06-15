-- Sample analytical queries for the Gold layer

-- 1. Total sales by customer city
SELECT
    c.city,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_revenue
FROM gold_customer_orders o
JOIN gold_customers c ON o.customer_id = c.customer_id
GROUP BY c.city
ORDER BY total_revenue DESC;

-- 2. Order conversion: users who viewed products vs users who ordered
-- (Simplified for demonstration)
WITH product_views AS (
    SELECT user_id, COUNT(*) as view_count
    FROM silver_user_activity
    WHERE activity_type = 'view_product'
    GROUP BY user_id
),
customer_orders AS (
    SELECT customer_id as user_id, COUNT(*) as order_count
    FROM silver_orders
    GROUP BY customer_id
)
SELECT
    v.user_id,
    v.view_count,
    COALESCE(o.order_count, 0) as order_count
FROM product_views v
LEFT JOIN customer_orders o ON v.user_id = o.user_id;

-- 3. Top selling products by category
SELECT
    p.category,
    p.name as product_name,
    SUM(oi.quantity) as units_sold
FROM gold_order_items oi
JOIN gold_products p ON oi.product_id = p.product_id
GROUP BY p.category, p.name
ORDER BY p.category, units_sold DESC;
