-- Enterprise Reporting Queries

-- 1. Top 10 Customers by Lifetime Value (LTV)
SELECT
    c.first_name,
    c.last_name,
    SUM(f.total_amount) as lifetime_spend,
    COUNT(f.order_id) as total_orders
FROM gold.fact_orders f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
WHERE f.order_status = 'completed'
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY lifetime_spend DESC
LIMIT 10;

-- 2. Product Performance by Category
SELECT
    p.category,
    p.name as product_name,
    SUM(f.total_amount) as total_revenue,
    COUNT(f.order_id) as units_sold
FROM gold.fact_orders f
JOIN gold.dim_products p ON f.order_id = p.product_key -- Simplified join for example
GROUP BY p.category, p.name
ORDER BY total_revenue DESC;

-- 3. Monthly Revenue Growth Rate
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', sale_date) as month,
        SUM(revenue) as revenue
    FROM gold.mart_sales_daily
    GROUP BY 1
)
SELECT
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) as prev_month_revenue,
    (revenue - LAG(revenue) OVER (ORDER BY month)) / NULLIF(LAG(revenue) OVER (ORDER BY month), 0) * 100 as growth_rate
FROM monthly_sales;
