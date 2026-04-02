DROP TABLE IF EXISTS mart_orders;

CREATE TABLE mart_orders AS
WITH order_items AS (
    SELECT
        order_id,
        SUM(price + freight_value) AS total_revenue
    FROM stg_order_items
    GROUP BY order_id
)

SELECT
    o.order_id,
    o.customer_id,

    -- ✅ ONLY DATE YOU SHOULD USE
    DATE(o.order_purchase_timestamp) AS order_date,

    -- ✅ CORE KPI FIELD (FIXED NULL ISSUE)
    COALESCE(oi.total_revenue, 0) AS total_revenue,

    -- ✅ STATUS (useful for filtering if needed)
    o.order_status

FROM stg_orders o

-- ✅ SAFE JOIN (NO DATA LOSS)
LEFT JOIN order_items oi
    ON o.order_id = oi.order_id;