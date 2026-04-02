DROP TABLE IF EXISTS fact_order_items;

CREATE TABLE fact_order_items AS
SELECT
    oi.order_id,                                     -- join key 1
    oi.order_item_id,
    oi.product_id,                                   -- join key 2
    oi.seller_id,                                    -- join key 3
    o.customer_id,                                   -- join key 4
    oi.price,
    oi.freight_value,
    oi.item_total,
    oi.product_category_name,
    o.delivery_status,
    o.actual_delivery_days,
    pr.avg_review_score,
    pr.total_paid
FROM int_order_items_enriched oi
LEFT JOIN int_orders_enriched    o  ON oi.order_id = o.order_id
LEFT JOIN int_payments_reviews   pr ON oi.order_id = pr.order_id;