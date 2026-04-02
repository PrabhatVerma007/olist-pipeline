DROP TABLE IF EXISTS int_order_items_enriched;

CREATE TABLE int_order_items_enriched AS
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value AS item_total,

    p.product_category_name,
    p.product_weight_g,

    s.seller_city,
    s.seller_state,
    s.seller_zip_code_prefix

FROM stg_order_items oi
LEFT JOIN stg_products p ON oi.product_id = p.product_id
LEFT JOIN stg_sellers s  ON oi.seller_id  = s.seller_id;