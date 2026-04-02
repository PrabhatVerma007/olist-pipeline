DROP TABLE IF EXISTS mart_payments;

CREATE TABLE mart_payments AS
SELECT
    order_id,                                        -- PK/FK
    customer_id,
    total_paid,
    max_installments,
    payment_types_used,
    CASE
        WHEN max_installments = 1  THEN 'full_payment'
        WHEN max_installments <= 6 THEN 'short_emi'
        ELSE 'long_emi'
    END                                              AS payment_segment,
    RANK() OVER (ORDER BY total_paid DESC)           AS spend_rank
FROM int_payments_reviews;