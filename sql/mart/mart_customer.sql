DROP TABLE IF EXISTS mart_customer;

CREATE TABLE mart_customer AS
SELECT
    cg.customer_id,                                  -- PK
    cg.customer_unique_id,
    cg.customer_city,
    cg.customer_state,
    cg.lat,
    cg.lng,
    COUNT(DISTINCT o.order_id)                       AS total_orders,
    SUM(oi.price)                                    AS lifetime_value,
    ROUND(AVG(oi.price), 2)                          AS avg_order_value,
    SUM(CASE WHEN o.delivery_status = 'late'
             THEN 1 ELSE 0 END)                      AS late_deliveries,
    RANK() OVER (ORDER BY SUM(oi.price) DESC)        AS ltv_rank,
    NTILE(4) OVER (ORDER BY SUM(oi.price) DESC)      AS ltv_quartile
FROM int_customers_geo cg
LEFT JOIN int_orders_enriched o
    ON cg.customer_id = o.customer_id
LEFT JOIN int_order_items_enriched oi
    ON o.order_id = oi.order_id
GROUP BY cg.customer_id, cg.customer_unique_id,
         cg.customer_city, cg.customer_state,
         cg.lat, cg.lng;