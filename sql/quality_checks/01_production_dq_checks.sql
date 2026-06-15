-- Production Data Quality Checks
-- These queries should return 0 rows if quality is high.

-- 1. Uniqueness check on dim_customers
SELECT customer_key, COUNT(*)
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- 2. Null check on critical Fact fields
SELECT order_id
FROM gold.fact_orders
WHERE customer_key IS NULL OR total_amount IS NULL;

-- 3. Referential integrity: Orders without customers
SELECT f.order_id
FROM gold.fact_orders f
LEFT JOIN gold.dim_customers d ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL;

-- 4. Financial Reconciliation: Paid amount vs Order total
-- Allowing for small variance if applicable
SELECT order_id, total_amount, paid_amount
FROM gold.fact_orders
WHERE ABS(total_amount - paid_amount) > 0.01 AND order_status = 'completed';
