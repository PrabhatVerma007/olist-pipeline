DROP TABLE IF EXISTS mart_seller;

CREATE TABLE mart_seller AS
SELECT
    oi.seller_id,                                    -- PK
    oi.seller_city,
    oi.seller_state,
    COUNT(DISTINCT oi.order_id)                      AS total_orders,
    ROUND(SUM(oi.price), 2)                          AS total_gmv,
    ROUND(AVG(oi.price), 2)                          AS avg_item_price,
    ROUND(AVG(o.actual_delivery_days), 1)            AS avg_delivery_days,
    SUM(CASE WHEN o.delivery_status = 'late'
             THEN 1 ELSE 0 END)                      AS late_orders,
    RANK() OVER (ORDER BY SUM(oi.price) DESC)        AS gmv_rank
FROM int_order_items_enriched oi
LEFT JOIN int_orders_enriched o ON oi.order_id = o.order_id
GROUP BY oi.seller_id, oi.seller_city, oi.seller_state;