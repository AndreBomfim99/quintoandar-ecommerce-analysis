-- =========================================================
-- STAGING: GEOLOCATION
-- =========================================================
-- Description: Cleans and enriches geolocation data
-- Primary source: olist_raw.geolocation
-- Secondary source: basedosdados.br_bd_diretorios_brasil.cep
-- Destination: olist_staging.stg_geolocation
-- 
-- Strategy: Use Base dos Dados to get official city names
-- and more accurate coordinates
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_staging.stg_geolocation` AS

WITH olist_source AS (
  SELECT *
  FROM `quintoandar-ecommerce-analysis.olist_raw.geolocation`
),

olist_prepared AS (
  SELECT
    CAST(geolocation_zip_code_prefix AS STRING) AS zip_code_prefix,
    
    TRIM(INITCAP(geolocation_city)) AS olist_city,
    UPPER(TRIM(geolocation_state)) AS olist_state,
    
    APPROX_QUANTILES(geolocation_lat, 100)[OFFSET(50)] AS olist_lat,
    APPROX_QUANTILES(geolocation_lng, 100)[OFFSET(50)] AS olist_lng,
    
    COUNT(*) AS olist_occurrences
    
  FROM olist_source
  
  WHERE 1=1
    AND geolocation_zip_code_prefix IS NOT NULL
    AND geolocation_lat BETWEEN -34 AND 5     
    AND geolocation_lng BETWEEN -74 AND -34   
    AND NOT (geolocation_lat = 0 AND geolocation_lng = 0) 
    
    AND UPPER(TRIM(geolocation_state)) IN (
      'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
      'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
      'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SE', 'SP', 'TO'
    )
  
  GROUP BY zip_code_prefix, olist_city, olist_state
),

basedosdados_prepared AS (
  SELECT
    SUBSTR(cep, 1, 5) AS cep_prefix,
    
    ANY_VALUE(localidade) AS bd_city,
    ANY_VALUE(sigla_uf) AS bd_state,
    ANY_VALUE(id_municipio) AS bd_municipality_id,
    
    ANY_VALUE(ST_Y(centroide)) AS bd_lat,  
    ANY_VALUE(ST_X(centroide)) AS bd_lng   
    
  FROM `basedosdados.br_bd_diretorios_brasil.cep`
  
  WHERE 1=1
    AND cep IS NOT NULL
    AND sigla_uf IS NOT NULL
    AND centroide IS NOT NULL
    AND ST_Y(centroide) BETWEEN -34 AND 5     
    AND ST_X(centroide) BETWEEN -74 AND -34    
  
  GROUP BY cep_prefix
),

joined AS (
  SELECT
    o.zip_code_prefix,
    
    o.olist_city,
    o.olist_state,
    o.olist_lat,
    o.olist_lng,
    o.olist_occurrences,
    
    b.bd_city,
    b.bd_state,
    b.bd_municipality_id,
    b.bd_lat,
    b.bd_lng,
    
    CASE WHEN b.cep_prefix IS NOT NULL THEN TRUE ELSE FALSE END AS has_basedosdados_match
    
  FROM olist_prepared o
  LEFT JOIN basedosdados_prepared b
    ON o.zip_code_prefix = b.cep_prefix
),

final_selection AS (
  SELECT
    zip_code_prefix AS geolocation_zip_code_prefix,
    
    COALESCE(bd_city, olist_city) AS geolocation_city,
    
    COALESCE(bd_state, olist_state) AS geolocation_state,
    
    COALESCE(bd_lat, olist_lat) AS geolocation_lat,
    COALESCE(bd_lng, olist_lng) AS geolocation_lng,
    
    bd_municipality_id AS municipality_id,
    
    CASE 
      WHEN bd_city IS NOT NULL THEN 'basedosdados'
      ELSE 'olist'
    END AS data_source,
    
    CASE 
      WHEN COALESCE(bd_state, olist_state) IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
      WHEN COALESCE(bd_state, olist_state) IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
      WHEN COALESCE(bd_state, olist_state) IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
      WHEN COALESCE(bd_state, olist_state) IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
      WHEN COALESCE(bd_state, olist_state) IN ('PR', 'RS', 'SC') THEN 'Sul'
      ELSE 'Desconhecido'
    END AS geolocation_region,
    
    has_basedosdados_match
    
  FROM joined
),

deduplicated AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (
        PARTITION BY geolocation_zip_code_prefix 
        ORDER BY 
          CASE WHEN data_source = 'basedosdados' THEN 1 ELSE 2 END,
          geolocation_state
      ) AS row_num
    FROM final_selection
  )
  WHERE row_num = 1
)

SELECT 
  geolocation_zip_code_prefix,
  geolocation_city,
  geolocation_state,
  geolocation_lat,
  geolocation_lng,
  municipality_id,
  data_source,
  geolocation_region
FROM deduplicated

ORDER BY geolocation_state, geolocation_zip_code_prefix;


/*
-- VALIDAÇÃO: Percentual de nulos e cobertura
SELECT 
  'stg_geolocation' as tabela,
  COUNT(*) as total_ceps,
  COUNTIF(geolocation_zip_code_prefix IS NULL) * 100.0 / COUNT(*) AS pct_null_zip,
  COUNTIF(geolocation_city IS NULL) * 100.0 / COUNT(*) AS pct_null_city,
  COUNTIF(geolocation_state IS NULL) * 100.0 / COUNT(*) AS pct_null_state,
  COUNTIF(geolocation_lat IS NULL) * 100.0 / COUNT(*) AS pct_null_lat,
  COUNTIF(geolocation_lng IS NULL) * 100.0 / COUNT(*) AS pct_null_lng,
  COUNTIF(municipality_id IS NULL) * 100.0 / COUNT(*) AS pct_null_municipality
FROM `quintoandar-ecommerce-analysis.olist_staging.stg_geolocation`;

SELECT 
  data_source,
  COUNT(*) AS total_ceps,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentual
FROM `quintoandar-ecommerce-analysis.olist_staging.stg_geolocation`
GROUP BY data_source
ORDER BY total_ceps DESC;

SELECT 
  geolocation_region,
  COUNT(*) AS total_ceps,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentual
FROM `quintoandar-ecommerce-analysis.olist_staging.stg_geolocation`
GROUP BY geolocation_region
ORDER BY total_ceps DESC;

SELECT 
  COUNT(*) AS total_ceps,
  COUNTIF(geolocation_lat BETWEEN -34 AND 5 AND geolocation_lng BETWEEN -74 AND -34) AS ceps_coordenadas_validas,
  ROUND(COUNTIF(geolocation_lat BETWEEN -34 AND 5 AND geolocation_lng BETWEEN -74 AND -34) * 100.0 / COUNT(*), 2) AS pct_validas
FROM `quintoandar-ecommerce-analysis.olist_staging.stg_geolocation`;
*/