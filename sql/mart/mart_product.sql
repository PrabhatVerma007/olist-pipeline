DROP TABLE IF EXISTS mart_product;

DROP TABLE IF EXISTS mart_product;

CREATE TABLE mart_product AS
SELECT
    oi.product_id,
    oi.product_category_name,

    YEAR(o.order_purchase_timestamp) AS order_year,
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS order_month,

    COUNT(DISTINCT oi.order_id) AS total_orders,
    SUM(oi.item_total) AS total_revenue,
    ROUND(AVG(oi.price), 2) AS avg_price,

    RANK() OVER (ORDER BY SUM(oi.item_total) DESC) AS revenue_rank

FROM int_order_items_enriched oi
LEFT JOIN int_orders_enriched o ON oi.order_id = o.order_id

GROUP BY
    oi.product_id,
    oi.product_category_name,
    order_year,
    order_month;