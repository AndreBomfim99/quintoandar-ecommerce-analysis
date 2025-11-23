-- =========================================================
-- STAGING: PRODUCTS
-- =========================================================
-- Description: Cleans and standardizes product data
-- Source: olist_raw.products
-- Destination: olist_staging.stg_products
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_staging.stg_products` AS

WITH source AS (
  SELECT *
  FROM `quintoandar-ecommerce-analysis.olist_raw.products`
),

cleaned AS (
  SELECT
    p.product_id,
    
    LOWER(TRIM(COALESCE(p.product_category_name, 'unknown'))) AS product_category_name,
    
    LOWER(TRIM(COALESCE(t.product_category_name_english, 'unknown'))) AS product_category_name_english,
    
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    
    CASE 
      WHEN p.product_weight_g > 0 AND p.product_weight_g <= 100000 
      THEN p.product_weight_g 
      ELSE NULL 
    END AS product_weight_g,
    
    CASE 
      WHEN p.product_length_cm > 0 AND p.product_length_cm <= 300 
      THEN p.product_length_cm 
      ELSE NULL 
    END AS product_length_cm,
    
    CASE 
      WHEN p.product_height_cm > 0 AND p.product_height_cm <= 300 
      THEN p.product_height_cm 
      ELSE NULL 
    END AS product_height_cm,
    
    CASE 
      WHEN p.product_width_cm > 0 AND p.product_width_cm <= 300 
      THEN p.product_width_cm 
      ELSE NULL 
    END AS product_width_cm
    
  FROM source p
  
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_raw.category_translation` t
    ON LOWER(TRIM(p.product_category_name)) = LOWER(TRIM(t.product_category_name))
  
  WHERE 1=1
    AND p.product_id IS NOT NULL
),

deduplicated AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (
        PARTITION BY product_id 
        ORDER BY 
          CASE WHEN product_weight_g IS NOT NULL THEN 1 ELSE 2 END,
          product_photos_qty DESC
      ) AS row_num
    FROM cleaned
  )
  WHERE row_num = 1
)

SELECT 
  product_id,
  product_category_name,
  product_category_name_english,
  product_name_lenght,
  product_description_lenght,
  product_photos_qty,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm
FROM deduplicated

ORDER BY product_category_name_english, product_id;

/*
-- VALIDAÇÃO: Percentual de nulos por coluna
SELECT 
  'stg_products' as tabela,
  COUNT(*) as total_registros,
  COUNTIF(product_id IS NULL) * 100.0 / COUNT(*) AS pct_null_product_id,
  COUNTIF(product_category_name IS NULL) * 100.0 / COUNT(*) AS pct_null_category,
  COUNTIF(product_category_name_english IS NULL) * 100.0 / COUNT(*) AS pct_null_category_english,
  COUNTIF(product_name_lenght IS NULL) * 100.0 / COUNT(*) AS pct_null_name_length,
  COUNTIF(product_description_lenght IS NULL) * 100.0 / COUNT(*) AS pct_null_desc_length,
  COUNTIF(product_photos_qty IS NULL) * 100.0 / COUNT(*) AS pct_null_photos,
  COUNTIF(product_weight_g IS NULL) * 100.0 / COUNT(*) AS pct_null_weight,
  COUNTIF(product_length_cm IS NULL) * 100.0 / COUNT(*) AS pct_null_length,
  COUNTIF(product_height_cm IS NULL) * 100.0 / COUNT(*) AS pct_null_height,
  COUNTIF(product_width_cm IS NULL) * 100.0 / COUNT(*) AS pct_null_width
FROM `quintoandar-ecommerce-analysis.olist_staging.stg_products`;
*/