DROP TABLE IF EXISTS mart_customer;

DROP TABLE IF EXISTS mart_customer;

CREATE TABLE mart_customer AS
SELECT
    cg.customer_id,
    cg.customer_unique_id,
    cg.customer_city,
    cg.customer_state,
    cg.lat,
    cg.lng,

    YEAR(o.order_purchase_timestamp) AS order_year,
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS order_month,

    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.item_total) AS lifetime_value,
    ROUND(AVG(oi.item_total), 2) AS avg_order_value,

    SUM(CASE WHEN o.delivery_status = 'late' THEN 1 ELSE 0 END) AS late_deliveries

FROM int_customers_geo cg
LEFT JOIN int_orders_enriched o ON cg.customer_id = o.customer_id
LEFT JOIN int_order_items_enriched oi ON o.order_id = oi.order_id

GROUP BY
    cg.customer_id,
    cg.customer_unique_id,
    cg.customer_city,
    cg.customer_state,
    cg.lat,
    cg.lng,
    order_year,
    order_month;