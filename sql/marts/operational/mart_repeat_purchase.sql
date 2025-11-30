-- =========================================================
-- MART: REPEAT PURCHASE
-- =========================================================
-- Description: Customer repeat behavior and timing analysis
-- Sources: stg_orders
-- Destination: olist_marts.mart_repeat_purchase
-- Granularity: 1 row per customer
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_repeat_purchase` AS

WITH customer_purchase_sequence AS (
  
  SELECT
    customer_id,
    order_id,
    order_purchase_timestamp,
    order_status,
    is_completed,
    
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp) AS purchase_number,
    
    FIRST_VALUE(order_purchase_timestamp) OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp) AS first_purchase_date,
    
    CASE 
      WHEN ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp) = 2 
      THEN order_purchase_timestamp 
    END AS second_purchase_date,
    
    DATE_TRUNC(FIRST_VALUE(order_purchase_timestamp) OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp), MONTH) AS cohort_month
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders`
  WHERE is_completed
),

customer_repeat_metrics AS (
  
  SELECT
    customer_id,
    
    COUNT(DISTINCT order_id) AS total_orders,
    MAX(purchase_number) AS max_purchase_sequence,
    
    MIN(first_purchase_date) AS first_purchase_date,
    MAX(second_purchase_date) AS second_purchase_date,
    
    MIN(cohort_month) AS cohort_month,
    
    CASE
      WHEN MAX(purchase_number) >= 2 THEN
        DATE_DIFF(MAX(second_purchase_date), MIN(first_purchase_date), DAY)
    END AS days_to_second_purchase,
    
    CASE WHEN MAX(purchase_number) >= 2 THEN 1 ELSE 0 END AS has_second_purchase,
    CASE WHEN MAX(purchase_number) >= 2 THEN 1 ELSE 0 END AS is_repeat_customer,
    
    CASE
      WHEN MAX(purchase_number) = 1 THEN '1x'
      WHEN MAX(purchase_number) = 2 THEN '2x'
      WHEN MAX(purchase_number) BETWEEN 3 AND 5 THEN '3-5x'
      WHEN MAX(purchase_number) BETWEEN 6 AND 10 THEN '6-10x'
      ELSE '11+'
    END AS purchase_frequency_bin,
    
    CASE
      WHEN MAX(purchase_number) >= 2 THEN
        CASE
          WHEN DATE_DIFF(MAX(second_purchase_date), MIN(first_purchase_date), DAY) BETWEEN 0 AND 30 THEN '0-30'
          WHEN DATE_DIFF(MAX(second_purchase_date), MIN(first_purchase_date), DAY) BETWEEN 31 AND 60 THEN '31-60'
          WHEN DATE_DIFF(MAX(second_purchase_date), MIN(first_purchase_date), DAY) BETWEEN 61 AND 90 THEN '61-90'
          WHEN DATE_DIFF(MAX(second_purchase_date), MIN(first_purchase_date), DAY) BETWEEN 91 AND 180 THEN '91-180'
          ELSE '180+'
        END
      ELSE 'no_second_purchase'
    END AS days_to_second_purchase_bin
    
  FROM customer_purchase_sequence
  GROUP BY customer_id
),

final_repeat_analysis AS (
  SELECT
    cr.customer_id,
    
    -- Core metrics
    cr.total_orders,
    cr.max_purchase_sequence,
    
    -- Temporal metrics
    cr.first_purchase_date,
    cr.second_purchase_date,
    cr.cohort_month,
    
    -- Repeat timing metrics
    cr.days_to_second_purchase,
    
    -- Behavioral flags
    cr.has_second_purchase,
    cr.is_repeat_customer,
    
    -- Segmentation
    cr.purchase_frequency_bin,
    cr.days_to_second_purchase_bin,
    
    -- Critical window flags
    CASE 
      WHEN cr.days_to_second_purchase <= 30 THEN 1 
      ELSE 0 
    END AS purchased_within_30_days,
    
    CASE 
      WHEN cr.days_to_second_purchase <= 60 THEN 1 
      ELSE 0 
    END AS purchased_within_60_days,
    
    CASE 
      WHEN cr.days_to_second_purchase <= 90 THEN 1 
      ELSE 0 
    END AS purchased_within_90_days,
    
    -- Customer tenure
    DATE_DIFF(CURRENT_DATE(), DATE(cr.first_purchase_date), DAY) AS days_since_first_purchase,
    
    -- Recency metrics
    CASE
      WHEN cr.second_purchase_date IS NOT NULL THEN
        DATE_DIFF(CURRENT_DATE(), DATE(cr.second_purchase_date), DAY)
    END AS days_since_second_purchase
    
  FROM customer_repeat_metrics cr
)

SELECT *
FROM final_repeat_analysis
ORDER BY cohort_month DESC, has_second_purchase DESC, days_to_second_purchase ASC;


/*
-- VALIDATION QUERIES
-- 1. Check overall repeat purchase rate
SELECT 
  COUNT(*) AS total_customers,
  SUM(is_repeat_customer) AS repeat_customers,
  ROUND(SUM(is_repeat_customer) * 100.0 / COUNT(*), 2) AS repeat_purchase_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_repeat_purchase`;

-- 2. Time-to-second-purchase distribution
SELECT 
  days_to_second_purchase_bin,
  COUNT(*) AS customer_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_repeat_purchase`
WHERE has_second_purchase = 1
GROUP BY days_to_second_purchase_bin
ORDER BY 
  CASE days_to_second_purchase_bin
    WHEN '0-30' THEN 1
    WHEN '31-60' THEN 2
    WHEN '61-90' THEN 3
    WHEN '91-180' THEN 4
    WHEN '180+' THEN 5
  END;

-- 3. Purchase frequency distribution
SELECT 
  purchase_frequency_bin,
  COUNT(*) AS customer_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_repeat_purchase`
GROUP BY purchase_frequency_bin
ORDER BY 
  CASE purchase_frequency_bin
    WHEN '1x' THEN 1
    WHEN '2x' THEN 2
    WHEN '3-5x' THEN 3
    WHEN '6-10x' THEN 4
    WHEN '11+' THEN 5
  END;

-- 4. Cohort analysis - repeat rate by month
SELECT 
  cohort_month,
  COUNT(*) AS total_customers,
  SUM(has_second_purchase) AS second_purchases,
  ROUND(SUM(has_second_purchase) * 100.0 / COUNT(*), 2) AS second_purchase_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_repeat_purchase`
GROUP BY cohort_month
ORDER BY cohort_month;

-- 5. Critical window analysis
SELECT 
  SUM(purchased_within_30_days) AS within_30_days,
  SUM(purchased_within_60_days) AS within_60_days,
  SUM(purchased_within_90_days) AS within_90_days,
  COUNT(*) AS total_repeat_customers,
  ROUND(SUM(purchased_within_30_days) * 100.0 / COUNT(*), 2) AS pct_within_30_days,
  ROUND(SUM(purchased_within_60_days) * 100.0 / COUNT(*), 2) AS pct_within_60_days,
  ROUND(SUM(purchased_within_90_days) * 100.0 / COUNT(*), 2) AS pct_within_90_days
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_repeat_purchase`
WHERE has_second_purchase = 1;

-- 6. Statistical summary of time-to-second-purchase
SELECT 
  COUNT(*) AS repeat_customers,
  ROUND(AVG(days_to_second_purchase), 2) AS avg_days_to_second_purchase,
  APPROX_QUANTILES(days_to_second_purchase, 100)[OFFSET(50)] AS median_days_to_second_purchase,
  MIN(days_to_second_purchase) AS min_days_to_second_purchase,
  MAX(days_to_second_purchase) AS max_days_to_second_purchase
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_repeat_purchase`
WHERE has_second_purchase = 1;
*/