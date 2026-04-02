DROP TABLE IF EXISTS mart_delivery_reviews;

CREATE TABLE mart_delivery_reviews AS
SELECT
    o.order_id,                                      -- PK
    o.customer_id,
    o.delivery_status,
    o.actual_delivery_days,
    o.early_vs_late_days,
    pr.avg_review_score,
    pr.review_response_days,
    CASE
        WHEN pr.avg_review_score >= 4 THEN 'positive'
        WHEN pr.avg_review_score >= 3 THEN 'neutral'
        ELSE 'negative'
    END                                              AS sentiment,
    AVG(pr.avg_review_score) OVER ()                 AS global_avg_score,
    pr.avg_review_score -
        AVG(pr.avg_review_score) OVER ()             AS score_vs_global
FROM int_orders_enriched o
LEFT JOIN int_payments_reviews pr ON o.order_id = pr.order_id;