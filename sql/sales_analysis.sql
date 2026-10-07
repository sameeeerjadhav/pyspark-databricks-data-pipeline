-- Monthly revenue from recognized orders.
SELECT
    year,
    month,
    total_orders,
    total_revenue,
    average_order_value
FROM monthly_sales_summary
ORDER BY year, month;

-- Daily revenue.
SELECT
    date,
    total_orders,
    total_items,
    gross_sales,
    discount_amount,
    net_sales,
    average_order_value
FROM daily_sales
ORDER BY date;

-- Overall average order value across recognized orders.
SELECT
    ROUND(SUM(net_sales) / SUM(total_orders), 2) AS average_order_value,
    SUM(total_orders) AS total_orders,
    SUM(net_sales) AS total_revenue
FROM daily_sales;

-- Cancelled orders as a percentage of Silver orders, including non-revenue statuses.
SELECT
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
    ROUND(
        SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS cancelled_order_percentage
FROM silver_orders;

-- Month-over-month revenue change.
WITH monthly AS (
    SELECT
        year,
        month,
        total_revenue,
        LAG(total_revenue) OVER (ORDER BY year, month) AS previous_revenue
    FROM monthly_sales_summary
)
SELECT
    year,
    month,
    total_revenue,
    previous_revenue,
    ROUND(total_revenue - previous_revenue, 2) AS month_over_month_change
FROM monthly
ORDER BY year, month;

-- Revenue growth rate against the previous month.
WITH monthly AS (
    SELECT
        year,
        month,
        total_revenue,
        LAG(total_revenue) OVER (ORDER BY year, month) AS previous_revenue
    FROM monthly_sales_summary
)
SELECT
    year,
    month,
    total_revenue,
    previous_revenue,
    ROUND(
        (total_revenue - previous_revenue) / NULLIF(previous_revenue, 0) * 100,
        2
    ) AS revenue_growth_percentage
FROM monthly
ORDER BY year, month;
