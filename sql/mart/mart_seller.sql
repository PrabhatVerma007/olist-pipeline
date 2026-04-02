DROP TABLE IF EXISTS mart_seller;

DROP TABLE IF EXISTS mart_seller;

CREATE TABLE mart_seller AS
SELECT
    oi.seller_id,
    oi.seller_city,
    oi.seller_state,

    YEAR(o.order_purchase_timestamp) AS order_year,
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS order_month,

    COUNT(DISTINCT oi.order_id) AS total_orders,
    SUM(oi.item_total) AS total_gmv,
    ROUND(AVG(oi.price), 2) AS avg_item_price,

    ROUND(AVG(CASE WHEN o.actual_delivery_days IS NOT NULL
                   THEN o.actual_delivery_days END), 1) AS avg_delivery_days,

    SUM(CASE WHEN o.delivery_status = 'late' THEN 1 ELSE 0 END) AS late_orders

FROM int_order_items_enriched oi
LEFT JOIN int_orders_enriched o ON oi.order_id = o.order_id

GROUP BY
    oi.seller_id,
    oi.seller_city,
    oi.seller_state,
    order_year,
    order_month;