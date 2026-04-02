DROP TABLE IF EXISTS mart_payments;

DROP TABLE IF EXISTS mart_payments;

CREATE TABLE mart_payments AS
SELECT
    pr.order_id,
    pr.customer_id,
    pr.total_paid,
    pr.max_installments,
    pr.payment_types_used,

    YEAR(o.order_purchase_timestamp) AS order_year,
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS order_month,

    CASE
        WHEN pr.max_installments = 1 THEN 'full_payment'
        WHEN pr.max_installments <= 6 THEN 'short_emi'
        ELSE 'long_emi'
    END AS payment_segment

FROM int_payments_reviews pr
LEFT JOIN int_orders_enriched o ON pr.order_id = o.order_id;