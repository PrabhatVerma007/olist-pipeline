DROP TABLE IF EXISTS fact_order_items;

CREATE TABLE fact_order_items AS
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,

    oi.price,
    oi.freight_value,
    oi.item_total,
    oi.product_category_name,

    o.delivery_status,
    o.actual_delivery_days,

    pr.avg_review_score,

    DATE(o.order_purchase_timestamp) AS order_date,
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS order_month,
    YEAR(o.order_purchase_timestamp) AS order_year

FROM int_order_items_enriched oi

-- Keep this (but ensure no filtering inside int_orders_enriched)
INNER JOIN int_orders_enriched o
    ON oi.order_id = o.order_id

-- FIX: do not filter facts
LEFT JOIN mart_customer mc
    ON o.customer_id = mc.customer_id

LEFT JOIN int_payments_reviews pr
    ON oi.order_id = pr.order_id;