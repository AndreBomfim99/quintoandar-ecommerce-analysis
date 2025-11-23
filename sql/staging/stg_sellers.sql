-- =========================================================
-- STAGING: SELLERS
-- =========================================================
-- Description: Cleans and standardizes seller data
-- Source: olist_raw.sellers
-- Destination: olist_staging.stg_sellers
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_staging.stg_sellers` AS

WITH source AS (
  SELECT *
  FROM `quintoandar-ecommerce-analysis.olist_raw.sellers`
),

cleaned AS (
  SELECT
    seller_id,
    
    CAST(seller_zip_code_prefix AS STRING) AS seller_zip_code_prefix,
    TRIM(INITCAP(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state,
    
    CASE 
      WHEN UPPER(TRIM(seller_state)) IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
      WHEN UPPER(TRIM(seller_state)) IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
      WHEN UPPER(TRIM(seller_state)) IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
      WHEN UPPER(TRIM(seller_state)) IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
      WHEN UPPER(TRIM(seller_state)) IN ('PR', 'RS', 'SC') THEN 'Sul'
      ELSE 'Desconhecido'
    END AS seller_region
    
  FROM source
  
  WHERE 1=1
    AND seller_id IS NOT NULL
    
    AND UPPER(TRIM(seller_state)) IN (
      'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
      'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
      'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SE', 'SP', 'TO'
    )
    
    AND CAST(seller_zip_code_prefix AS INT64) BETWEEN 1000 AND 99999
),

deduplicated AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (
        PARTITION BY seller_id 
        ORDER BY seller_state, seller_city
      ) AS row_num
    FROM cleaned
  )
  WHERE row_num = 1
)

SELECT 
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state,
  seller_region
FROM deduplicated

ORDER BY seller_state, seller_city;


/*
-- VALIDAÇÃO: Percentual de nulos por coluna
SELECT 
  'stg_sellers' as tabela,
  COUNT(*) as total_registros,
  COUNTIF(seller_id IS NULL) * 100.0 / COUNT(*) AS pct_null_seller_id,
  COUNTIF(seller_zip_code_prefix IS NULL) * 100.0 / COUNT(*) AS pct_null_zip,
  COUNTIF(seller_city IS NULL) * 100.0 / COUNT(*) AS pct_null_city,
  COUNTIF(seller_state IS NULL) * 100.0 / COUNT(*) AS pct_null_state,
  COUNTIF(seller_region IS NULL) * 100.0 / COUNT(*) AS pct_null_region
FROM `quintoandar-ecommerce-analysis.olist_staging.stg_sellers`;
*/