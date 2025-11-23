-- =========================================================
-- STAGING: CUSTOMERS
-- =========================================================
-- Description: Cleans and standardizes customer data
-- Source: olist_raw.customers
-- Destination: olist_staging.stg_customers
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_staging.stg_customers` AS

WITH source AS (
  SELECT *
  FROM `quintoandar-ecommerce-analysis.olist_raw.customers`
),

cleaned AS (
  SELECT
    customer_id,
    customer_unique_id,
    
    CAST(customer_zip_code_prefix AS STRING) AS customer_zip_code_prefix,
    TRIM(INITCAP(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state,
    
    CASE 
      WHEN UPPER(TRIM(customer_state)) IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
      WHEN UPPER(TRIM(customer_state)) IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
      WHEN UPPER(TRIM(customer_state)) IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
      WHEN UPPER(TRIM(customer_state)) IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
      WHEN UPPER(TRIM(customer_state)) IN ('PR', 'RS', 'SC') THEN 'Sul'
      ELSE 'Desconhecido'
    END AS customer_region
    
  FROM source
  
  WHERE 1=1
    AND customer_id IS NOT NULL
    AND customer_unique_id IS NOT NULL
    
    AND UPPER(TRIM(customer_state)) IN (
      'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
      'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
      'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SE', 'SP', 'TO'
    )
    
    AND CAST(customer_zip_code_prefix AS INT64) BETWEEN 1000 AND 99999
),

deduplicated AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY customer_unique_id
      ) AS row_num
    FROM cleaned
  )
  WHERE row_num = 1
)

SELECT 
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  customer_region
FROM deduplicated

ORDER BY customer_state, customer_city;


/*
-- VALIDAÇÃO: Percentual de nulos por coluna
SELECT 
  'stg_customers' as tabela,
  COUNT(*) as total_registros,
  COUNTIF(customer_id IS NULL) * 100.0 / COUNT(*) AS pct_null_customer_id,
  COUNTIF(customer_unique_id IS NULL) * 100.0 / COUNT(*) AS pct_null_customer_unique_id,
  COUNTIF(customer_zip_code_prefix IS NULL) * 100.0 / COUNT(*) AS pct_null_zip,
  COUNTIF(customer_city IS NULL) * 100.0 / COUNT(*) AS pct_null_city,
  COUNTIF(customer_state IS NULL) * 100.0 / COUNT(*) AS pct_null_state,
  COUNTIF(customer_region IS NULL) * 100.0 / COUNT(*) AS pct_null_region
FROM `quintoandar-ecommerce-analysis.olist_staging.stg_customers`;
*/