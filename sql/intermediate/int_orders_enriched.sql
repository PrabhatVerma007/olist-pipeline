DROP TABLE IF EXISTS int_orders_enriched;

CREATE TABLE int_orders_enriched AS
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    DATEDIFF(order_delivered_customer_date,
             order_purchase_timestamp) AS actual_delivery_days,

    DATEDIFF(order_estimated_delivery_date,
             order_delivered_customer_date) AS early_vs_late_days,

    CASE
        WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 'on_time'
        WHEN order_delivered_customer_date IS NULL THEN 'not_delivered'
        ELSE 'late'
    END AS delivery_status

FROM stg_orders;