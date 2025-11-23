-- =========================================================
-- STAGING: PAYMENTS
-- =========================================================
-- Description: Cleans and standardizes payment data
-- Source: olist_raw.payments
-- Destination: olist_staging.stg_payments
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_staging.stg_payments` AS

WITH source AS (
  SELECT *
  FROM `quintoandar-ecommerce-analysis.olist_raw.payments`
),

cleaned AS (
  SELECT
    order_id,
    payment_sequential,
    
    LOWER(TRIM(payment_type)) AS payment_type,
    
    COALESCE(payment_installments, 1) AS payment_installments,
    
    payment_value,
    
    LOWER(TRIM(payment_type)) = 'credit_card' AS is_credit_card,
    LOWER(TRIM(payment_type)) = 'boleto' AS is_boleto
    
  FROM source
  
  WHERE 1=1
    AND order_id IS NOT NULL
    AND payment_value > 0
    AND COALESCE(payment_installments, 1) >= 1
    AND LOWER(TRIM(payment_type)) IN ('credit_card', 'debit_card', 'boleto', 'voucher')
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
        PARTITION BY order_id, payment_sequential 
        ORDER BY payment_value DESC
      ) AS row_num
    FROM validated
  )
  WHERE row_num = 1
)

SELECT 
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value,
  is_credit_card,
  is_boleto
FROM deduplicated

ORDER BY order_id, payment_sequential;

/*
-- VALIDAÇÃO: Percentual de nulos por coluna
SELECT 
  'stg_payments' as tabela,
  COUNT(*) as total_registros,
  COUNTIF(order_id IS NULL) * 100.0 / COUNT(*) AS pct_null_order_id,
  COUNTIF(payment_sequential IS NULL) * 100.0 / COUNT(*) AS pct_null_sequential,
  COUNTIF(payment_type IS NULL) * 100.0 / COUNT(*) AS pct_null_type,
  COUNTIF(payment_installments IS NULL) * 100.0 / COUNT(*) AS pct_null_installments,
  COUNTIF(payment_value IS NULL) * 100.0 / COUNT(*) AS pct_null_value
FROM `quintoandar-ecommerce-analysis.olist_staging.stg_payments`;
*/