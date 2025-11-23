-- =========================================================
-- STAGING: ORDER ITEMS
-- =========================================================
-- Description: Cleans and standardizes order items data
-- Source: olist_raw.order_items
-- Destination: olist_staging.stg_order_items
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` AS

WITH source AS (
  SELECT *
  FROM `quintoandar-ecommerce-analysis.olist_raw.order_items`
),

cleaned AS (
  SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', shipping_limit_date) AS shipping_limit_date,
    
    price,
    freight_value,
    
    price + freight_value AS total_item_value
    
  FROM source
  
  WHERE 1=1
    AND order_id IS NOT NULL
    AND product_id IS NOT NULL
    AND seller_id IS NOT NULL
    
    AND price >= 0
    AND freight_value >= 0
    
    AND price <= 50000
),

validated AS (
  SELECT 
    c.*
  FROM cleaned c
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
    ON c.order_id = o.order_id
),

deduplicated AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (
        PARTITION BY order_id, order_item_id 
        ORDER BY shipping_limit_date
      ) AS row_num
    FROM validated
  )
  WHERE row_num = 1
)

SELECT 
  order_id,
  order_item_id,
  product_id,
  seller_id,
  shipping_limit_date,
  price,
  freight_value,
  total_item_value
FROM deduplicated

ORDER BY order_id, order_item_id;

/*
-- VALIDAÇÃO: Percentual de nulos por coluna
SELECT 
  'stg_order_items' as tabela,
  COUNT(*) as total_registros,
  COUNTIF(order_id IS NULL) * 100.0 / COUNT(*) AS pct_null_order_id,
  COUNTIF(order_item_id IS NULL) * 100.0 / COUNT(*) AS pct_null_item_id,
  COUNTIF(product_id IS NULL) * 100.0 / COUNT(*) AS pct_null_product_id,
  COUNTIF(seller_id IS NULL) * 100.0 / COUNT(*) AS pct_null_seller_id,
  COUNTIF(shipping_limit_date IS NULL) * 100.0 / COUNT(*) AS pct_null_shipping_date,
  COUNTIF(price IS NULL) * 100.0 / COUNT(*) AS pct_null_price,
  COUNTIF(freight_value IS NULL) * 100.0 / COUNT(*) AS pct_null_freight
FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items`;
*/