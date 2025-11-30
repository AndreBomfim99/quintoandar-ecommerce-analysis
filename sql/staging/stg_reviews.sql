-- =========================================================
-- STAGING: REVIEWS
-- =========================================================
-- Description: Cleans and standardizes review data
-- Source: olist_raw.reviews
-- Destination: olist_staging.stg_reviews
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_staging.stg_reviews` AS
WITH
  source AS (
    SELECT
      *
    FROM
      `quintoandar-ecommerce-analysis`.olist_raw.reviews
  ),
  cleaned AS (
    SELECT
      review_id,
      order_id,
      review_score,
      CASE
        WHEN review_comment_title IS NOT NULL THEN TRIM(review_comment_title)
        ELSE NULL
      END AS review_comment_title,
      CASE
        WHEN review_comment_message IS NOT NULL THEN TRIM(review_comment_message)
        ELSE NULL
      END AS review_comment_message,
      review_creation_date,
      review_answer_timestamp,
      review_comment_message IS NOT NULL AND TRIM(review_comment_message) != '' AS has_comment
    FROM
      source
    WHERE
      1 = 1 AND review_id IS NOT NULL AND order_id IS NOT NULL AND review_score BETWEEN 1 AND 5 AND review_creation_date IS NOT NULL
  ),
  validated AS (
    SELECT
      c.*
    FROM
      cleaned AS c
      INNER JOIN
      `quintoandar-ecommerce-analysis`.olist_staging.stg_orders AS o
      ON c.order_id = o.order_id
  ),
  deduplicated AS (
    SELECT
      * EXCEPT (row_num)
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY review_id
            ORDER BY review_creation_date DESC) AS row_num
        FROM
          validated
      )
    WHERE
      row_num = 1
  )
SELECT
  review_id,
  order_id,
  review_score,
  review_comment_title,
  review_comment_message,
  review_creation_date,
  review_answer_timestamp,
  has_comment
FROM
  deduplicated
ORDER BY review_creation_date DESC;


/*
-- VALIDAÇÃO: Percentual de nulos por coluna
SELECT 
  'stg_reviews' as tabela,
  COUNT(*) as total_registros,
  COUNTIF(review_id IS NULL) * 100.0 / COUNT(*) AS pct_null_review_id,
  COUNTIF(order_id IS NULL) * 100.0 / COUNT(*) AS pct_null_order_id,
  COUNTIF(review_score IS NULL) * 100.0 / COUNT(*) AS pct_null_score,
  COUNTIF(review_comment_title IS NULL) * 100.0 / COUNT(*) AS pct_null_title,
  COUNTIF(review_comment_message IS NULL) * 100.0 / COUNT(*) AS pct_null_message,
  COUNTIF(review_creation_date IS NULL) * 100.0 / COUNT(*) AS pct_null_creation,
  COUNTIF(review_answer_timestamp IS NULL) * 100.0 / COUNT(*) AS pct_null_answer
FROM `quintoandar-ecommerce-analysis.olist_staging.stg_reviews`;
*/