-- Top 10 products by net revenue.
SELECT
    product_id,
    product_name,
    category,
    units_sold,
    gross_revenue,
    discount_amount,
    net_revenue
FROM product_sales_summary
ORDER BY net_revenue DESC
LIMIT 10;

-- Revenue by category.
SELECT
    category,
    total_orders,
    units_sold,
    revenue
FROM category_sales_summary
ORDER BY revenue DESC;

-- Best-selling products by units sold.
SELECT
    product_id,
    product_name,
    category,
    units_sold,
    net_revenue
FROM product_sales_summary
ORDER BY units_sold DESC, net_revenue DESC
LIMIT 10;
