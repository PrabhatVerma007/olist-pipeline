DROP TABLE IF EXISTS int_customers_geo;

CREATE TABLE int_customers_geo AS
SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,

    AVG(g.geolocation_lat) AS lat,
    AVG(g.geolocation_lng) AS lng

FROM stg_customers c
LEFT JOIN stg_geolocation g
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix

GROUP BY
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state;