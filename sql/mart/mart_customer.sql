DROP TABLE IF EXISTS mart_customer;

CREATE TABLE mart_customer AS
SELECT
    cg.customer_id,
    cg.customer_unique_id,

    MIN(cg.customer_city)  AS customer_city,
    MIN(cg.customer_state) AS customer_state,
    ROUND(AVG(cg.lat), 5)  AS lat,
    ROUND(AVG(cg.lng), 5)  AS lng,

    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.item_total) AS lifetime_value,
    ROUND(AVG(oi.item_total), 2) AS avg_order_value,

    SUM(CASE WHEN o.delivery_status = 'late' THEN 1 ELSE 0 END) AS late_deliveries

FROM int_customers_geo cg
LEFT JOIN int_orders_enriched o
    ON cg.customer_id = o.customer_id
LEFT JOIN int_order_items_enriched oi
    ON o.order_id = oi.order_id

GROUP BY
    cg.customer_id,
    cg.customer_unique_id;