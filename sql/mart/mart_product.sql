DROP TABLE IF EXISTS mart_product;

CREATE TABLE mart_product AS
SELECT
    product_id,                                      -- PK
    product_category_name,
    COUNT(DISTINCT order_id)                         AS total_orders,
    SUM(price)                                       AS total_revenue,
    ROUND(AVG(price), 2)                             AS avg_price,
    SUM(freight_value)                               AS total_freight,
    RANK() OVER (ORDER BY SUM(price) DESC)           AS revenue_rank,
    RANK() OVER (
        PARTITION BY product_category_name
        ORDER BY COUNT(DISTINCT order_id) DESC
    )                                                AS rank_in_category
FROM int_order_items_enriched
GROUP BY product_id, product_category_name;