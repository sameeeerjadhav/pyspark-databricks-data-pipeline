-- Top 10 customers by realized revenue.
SELECT
    customer_id,
    total_orders,
    total_spend,
    average_order_value,
    first_order_date,
    last_order_date
FROM customer_sales_summary
ORDER BY total_spend DESC
LIMIT 10;

-- Purchase frequency for customers who have more than one revenue order.
SELECT
    customer_id,
    total_orders,
    first_order_date,
    last_order_date,
    DATEDIFF(last_order_date, first_order_date) AS active_span_days,
    CASE
        WHEN total_orders <= 1 THEN NULL
        ELSE ROUND(DATEDIFF(last_order_date, first_order_date) / (total_orders - 1), 1)
    END AS average_days_between_orders
FROM customer_sales_summary
ORDER BY total_orders DESC;

-- Customers who never reached Silver orders.
SELECT
    customer_id,
    full_name,
    email,
    signup_date
FROM silver_customers AS customers
WHERE NOT EXISTS (
    SELECT 1
    FROM silver_orders AS orders
    WHERE orders.customer_id = customers.customer_id
);

-- Simple realized-value approximation. This is not a predictive lifetime-value model.
SELECT
    customer_id,
    total_spend AS realized_customer_value,
    average_order_value,
    total_orders
FROM customer_sales_summary
ORDER BY realized_customer_value DESC;
