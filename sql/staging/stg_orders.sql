-- =========================================================
-- STAGING: ORDERS
-- =========================================================
-- Description: Cleans and standardizes orders data
-- Source: olist_raw.orders
-- Destination: olist_staging.stg_orders
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_staging.stg_orders` AS

WITH source AS (
  SELECT *
  FROM `quintoandar-ecommerce-analysis.olist_raw.orders`
),

cleaned AS (
  SELECT
    order_id,
    customer_id,
    
    LOWER(TRIM(order_status)) AS order_status,
    
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp) AS order_purchase_timestamp,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_approved_at) AS order_approved_at,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_delivered_carrier_date) AS order_delivered_carrier_date,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_delivered_customer_date) AS order_delivered_customer_date,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_estimated_delivery_date) AS order_estimated_delivery_date,
    
    LOWER(TRIM(order_status)) = 'delivered' AS is_delivered,
    LOWER(TRIM(order_status)) IN ('delivered', 'invoiced') AS is_completed,
    LOWER(TRIM(order_status)) = 'canceled' AS is_canceled
    
  FROM source
  
  WHERE 1=1
    AND order_id IS NOT NULL
    AND customer_id IS NOT NULL
    
    AND PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp) <= CURRENT_TIMESTAMP()
),

validated AS (
  SELECT *
  FROM cleaned
  
  WHERE 1=1
    AND (order_approved_at IS NULL OR order_approved_at >= order_purchase_timestamp)
    
    AND (order_delivered_carrier_date IS NULL 
         OR order_delivered_carrier_date >= COALESCE(order_approved_at, order_purchase_timestamp))
    
    AND (order_delivered_customer_date IS NULL 
         OR order_delivered_customer_date >= COALESCE(order_delivered_carrier_date, order_approved_at, order_purchase_timestamp))
),

deduplicated AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (
        PARTITION BY order_id 
        ORDER BY order_purchase_timestamp
      ) AS row_num
    FROM validated
  )
  WHERE row_num = 1
)

SELECT 
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_carrier_date,
  order_delivered_customer_date,
  order_estimated_delivery_date,
  is_delivered,
  is_completed,
  is_canceled
FROM deduplicated

ORDER BY order_purchase_timestamp DESC;

/*
-- VALIDAÇÃO: Percentual de nulos por coluna
SELECT 
  'stg_orders' as tabela,
  COUNT(*) as total_registros,
  COUNTIF(order_id IS NULL) * 100.0 / COUNT(*) AS pct_null_order_id,
  COUNTIF(customer_id IS NULL) * 100.0 / COUNT(*) AS pct_null_customer_id,
  COUNTIF(order_status IS NULL) * 100.0 / COUNT(*) AS pct_null_status,
  COUNTIF(order_purchase_timestamp IS NULL) * 100.0 / COUNT(*) AS pct_null_purchase,
  COUNTIF(order_approved_at IS NULL) * 100.0 / COUNT(*) AS pct_null_approved,
  COUNTIF(order_delivered_carrier_date IS NULL) * 100.0 / COUNT(*) AS pct_null_carrier,
  COUNTIF(order_delivered_customer_date IS NULL) * 100.0 / COUNT(*) AS pct_null_delivered,
  COUNTIF(order_estimated_delivery_date IS NULL) * 100.0 / COUNT(*) AS pct_null_estimated
FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders`;
*/