-- =========================================================
-- MART: CUSTOMER COHORT RETENTION
-- =========================================================
-- Description: Cohort retention analysis tracking customer repurchase behavior
-- Based on: Analysis #2 (Cohort Retention Analysis)
-- Source: stg_orders, stg_payments
-- Destination: olist_marts.mart_customer_cohort_retention
-- Granularity: 1 row per cohort-month combination
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_customer_cohort_retention` AS

WITH customer_first_purchase AS (
  
  SELECT
    customer_id,
    MIN(order_purchase_timestamp) AS first_purchase_date,
    FORMAT_TIMESTAMP('%Y-%m', MIN(order_purchase_timestamp)) AS cohort_month
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders`
  WHERE is_delivered OR is_completed
  GROUP BY customer_id
),

customer_orders_with_cohort AS (
  
  SELECT
    o.customer_id,
    o.order_id,
    o.order_purchase_timestamp,
    FORMAT_TIMESTAMP('%Y-%m', o.order_purchase_timestamp) AS order_month,
    cfp.first_purchase_date,
    cfp.cohort_month,
    
    DATE_DIFF(
      DATE(o.order_purchase_timestamp),
      DATE(cfp.first_purchase_date),
      MONTH
    ) AS months_since_first_purchase
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN customer_first_purchase cfp
    ON o.customer_id = cfp.customer_id
  WHERE o.is_delivered OR o.is_completed
),

customer_revenue AS (
  SELECT
    o.order_id,
    o.customer_id,
    SUM(p.payment_value) AS order_revenue
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  WHERE o.is_delivered OR o.is_completed
  GROUP BY o.order_id, o.customer_id
),

cohort_size AS (
  
  SELECT
    cohort_month,
    COUNT(DISTINCT customer_id) AS cohort_size,
    MIN(first_purchase_date) AS cohort_start_date
  FROM customer_first_purchase
  GROUP BY cohort_month
),

retention_matrix AS (
  
  SELECT
    cowc.cohort_month,
    cowc.months_since_first_purchase,
    cs.cohort_size,
    cs.cohort_start_date,
    
    COUNT(DISTINCT cowc.customer_id) AS retained_customers,
    ROUND(COUNT(DISTINCT cowc.customer_id) * 100.0 / cs.cohort_size, 2) AS retention_rate,
    
    ROUND(SUM(cr.order_revenue), 2) AS cohort_revenue,
    ROUND(AVG(cr.order_revenue), 2) AS avg_revenue_per_order,
    ROUND(SUM(cr.order_revenue) / cs.cohort_size, 2) AS revenue_per_customer
    
  FROM customer_orders_with_cohort cowc
  INNER JOIN cohort_size cs
    ON cowc.cohort_month = cs.cohort_month
  LEFT JOIN customer_revenue cr
    ON cowc.order_id = cr.order_id
  GROUP BY 
    cowc.cohort_month,
    cowc.months_since_first_purchase,
    cs.cohort_size,
    cs.cohort_start_date
),

retention_with_cumulative AS (
  
  SELECT
    rm.*,
    
    SUM(rm.cohort_revenue) OVER (
      PARTITION BY rm.cohort_month 
      ORDER BY rm.months_since_first_purchase
    ) AS cumulative_revenue,
    
    CONCAT('M', CAST(rm.months_since_first_purchase AS STRING)) AS month_label
    
  FROM retention_matrix rm
)

SELECT
  cohort_month,
  cohort_start_date,
  cohort_size,
  months_since_first_purchase,
  month_label,
  retained_customers,
  retention_rate,
  cohort_revenue,
  cumulative_revenue,
  avg_revenue_per_order,
  revenue_per_customer
FROM retention_with_cumulative
ORDER BY cohort_month, months_since_first_purchase;


/*
-- VALIDATION QUERIES
-- 1. Retention matrix (cohort x month) - pivot view
SELECT
  cohort_month,
  cohort_size,
  MAX(CASE WHEN months_since_first_purchase = 0 THEN retention_rate END) AS M0,
  MAX(CASE WHEN months_since_first_purchase = 1 THEN retention_rate END) AS M1,
  MAX(CASE WHEN months_since_first_purchase = 2 THEN retention_rate END) AS M2,
  MAX(CASE WHEN months_since_first_purchase = 3 THEN retention_rate END) AS M3,
  MAX(CASE WHEN months_since_first_purchase = 6 THEN retention_rate END) AS M6,
  MAX(CASE WHEN months_since_first_purchase = 12 THEN retention_rate END) AS M12
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_cohort_retention`
GROUP BY cohort_month, cohort_size
ORDER BY cohort_month;

-- 2. Average retention curve
SELECT
  months_since_first_purchase,
  month_label,
  ROUND(AVG(retention_rate), 2) AS avg_retention_rate,
  COUNT(DISTINCT cohort_month) AS num_cohorts,
  SUM(retained_customers) AS total_retained_customers
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_cohort_retention`
GROUP BY months_since_first_purchase, month_label
ORDER BY months_since_first_purchase;

-- 3. Best and worst performing cohorts (Month 3 retention)
WITH cohort_m3 AS (
  SELECT
    cohort_month,
    cohort_size,
    retention_rate AS m3_retention_rate,
    retained_customers AS m3_retained_customers
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_cohort_retention`
  WHERE months_since_first_purchase = 3
)
SELECT
  'Best Cohorts' AS category,
  cohort_month,
  cohort_size,
  m3_retention_rate,
  m3_retained_customers
FROM cohort_m3
ORDER BY m3_retention_rate DESC
LIMIT 5

UNION ALL

SELECT
  'Worst Cohorts',
  cohort_month,
  cohort_size,
  m3_retention_rate,
  m3_retained_customers
FROM cohort_m3
ORDER BY m3_retention_rate ASC
LIMIT 5;

-- 4. Revenue retention analysis
SELECT
  months_since_first_purchase,
  month_label,
  ROUND(AVG(retention_rate), 2) AS avg_customer_retention,
  ROUND(SUM(cohort_revenue), 2) AS total_revenue,
  ROUND(AVG(revenue_per_customer), 2) AS avg_revenue_per_customer,
  COUNT(DISTINCT cohort_month) AS num_cohorts
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_cohort_retention`
GROUP BY months_since_first_purchase, month_label
ORDER BY months_since_first_purchase;

-- 5. Cohort performance summary
SELECT
  cohort_month,
  cohort_size,
  ROUND(MAX(cumulative_revenue), 2) AS total_cohort_revenue,
  ROUND(MAX(cumulative_revenue) / cohort_size, 2) AS ltv_per_customer,
  MAX(CASE WHEN months_since_first_purchase = 1 THEN retention_rate END) AS m1_retention,
  MAX(CASE WHEN months_since_first_purchase = 3 THEN retention_rate END) AS m3_retention,
  MAX(CASE WHEN months_since_first_purchase = 6 THEN retention_rate END) AS m6_retention
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_cohort_retention`
GROUP BY cohort_month, cohort_size
ORDER BY cohort_month;

-- 6. Retention rate decay analysis
WITH retention_decay AS (
  SELECT
    cohort_month,
    months_since_first_purchase,
    retention_rate,
    LAG(retention_rate) OVER (PARTITION BY cohort_month ORDER BY months_since_first_purchase) AS prev_month_retention,
    retention_rate - LAG(retention_rate) OVER (PARTITION BY cohort_month ORDER BY months_since_first_purchase) AS retention_change
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_cohort_retention`
)
SELECT
  months_since_first_purchase,
  ROUND(AVG(retention_rate), 2) AS avg_retention,
  ROUND(AVG(retention_change), 2) AS avg_retention_change,
  ROUND(MIN(retention_change), 2) AS max_decay,
  COUNT(DISTINCT cohort_month) AS num_cohorts
FROM retention_decay
WHERE prev_month_retention IS NOT NULL
GROUP BY months_since_first_purchase
ORDER BY months_since_first_purchase;
*/