-- Top products by revenue
SELECT
    product_id,
    SUM(quantity) AS units_sold,
    SUM(revenue) AS revenue
FROM retail_catalog.retail.orders
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;

-- Daily revenue
SELECT
    DATE(order_ts) AS order_date,
    COUNT(DISTINCT order_id) AS orders,
    SUM(revenue) AS revenue
FROM retail_catalog.retail.orders
GROUP BY DATE(order_ts)
ORDER BY order_date;
