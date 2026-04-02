DROP TABLE IF EXISTS int_payments_reviews;

CREATE TABLE int_payments_reviews AS
SELECT
    o.order_id,
    o.customer_id,

    SUM(p.payment_value) AS total_paid,
    MAX(p.payment_installments) AS max_installments,
    GROUP_CONCAT(DISTINCT p.payment_type) AS payment_types_used,

    AVG(r.review_score) AS avg_review_score,
    MIN(r.review_score) AS min_review_score,

    DATEDIFF(MAX(r.review_answer_timestamp),
             MIN(r.review_creation_date)) AS review_response_days

FROM stg_orders o
LEFT JOIN stg_order_payments p ON o.order_id = p.order_id
LEFT JOIN stg_order_reviews r  ON o.order_id = r.order_id

GROUP BY o.order_id, o.customer_id;