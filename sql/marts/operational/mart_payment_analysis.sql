-- =========================================================
-- MART: PAYMENT ANALYSIS
-- =========================================================
-- Description: Payment method analysis with conversion and risk metrics
-- Based on: Analysis #5 (Payment Analysis)
-- Source: stg_payments, stg_orders
-- Destination: olist_marts.mart_payment_analysis
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_payment_analysis` AS

WITH order_payments AS (

  SELECT
    p.order_id,
    
    ARRAY_AGG(p.payment_type ORDER BY p.payment_value DESC LIMIT 1)[OFFSET(0)] AS payment_method_primary,
    
    SUM(p.payment_value) AS total_payment_per_order,
    COUNT(DISTINCT p.payment_type) AS num_payment_methods,
    
    AVG(p.payment_installments) AS avg_installments,
    MAX(p.payment_installments) AS max_installments
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
  GROUP BY p.order_id
),

orders_with_payments AS (
  SELECT
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.is_delivered,
    o.is_completed,
    o.is_canceled,
    
    op.payment_method_primary,
    op.total_payment_per_order,
    op.num_payment_methods,
    op.avg_installments,
    op.max_installments,
    
    CASE
      WHEN op.avg_installments = 1 THEN '1x'
      WHEN op.avg_installments BETWEEN 2 AND 3 THEN '2-3x'
      WHEN op.avg_installments BETWEEN 4 AND 6 THEN '4-6x'
      WHEN op.avg_installments BETWEEN 7 AND 12 THEN '7-12x'
      WHEN op.avg_installments >= 13 THEN '13+x'
      ELSE 'Unknown'
    END AS parcelamento_bin,
    
    op.num_payment_methods > 1 AS has_multiple_payment_methods
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN order_payments op
    ON o.order_id = op.order_id
),

payment_method_metrics AS (
  
  SELECT
    payment_method_primary,
    
    COUNT(*) AS total_orders,
    COUNT(DISTINCT order_id) AS unique_orders,
    
    COUNTIF(is_delivered OR is_completed) AS successful_orders,
    COUNTIF(is_canceled) AS canceled_orders,
    
    ROUND(SUM(total_payment_per_order), 2) AS total_revenue,
    ROUND(AVG(total_payment_per_order), 2) AS avg_order_value,
    ROUND(MIN(total_payment_per_order), 2) AS min_order_value,
    ROUND(MAX(total_payment_per_order), 2) AS max_order_value,
    
    ROUND(AVG(avg_installments), 2) AS avg_installments_per_method,
    ROUND(MAX(max_installments), 2) AS max_installments_per_method,
    
    ROUND(COUNTIF(is_delivered OR is_completed) * 100.0 / COUNT(*), 2) AS conversion_rate,
    ROUND(COUNTIF(is_canceled) * 100.0 / COUNT(*), 2) AS cancellation_rate,
    
    COUNTIF(has_multiple_payment_methods) AS orders_with_multiple_methods
    
  FROM orders_with_payments
  GROUP BY payment_method_primary
),

installment_analysis AS (
  SELECT
    parcelamento_bin,
    COUNT(*) AS total_orders,
    ROUND(AVG(total_payment_per_order), 2) AS avg_aov,
    ROUND(SUM(total_payment_per_order), 2) AS total_revenue,
    ROUND(COUNTIF(is_delivered OR is_completed) * 100.0 / COUNT(*), 2) AS conversion_rate
  FROM orders_with_payments
  GROUP BY parcelamento_bin
),

payment_method_share AS (

  SELECT
    pmm.*,
    
    ROUND(pmm.total_orders * 100.0 / SUM(pmm.total_orders) OVER (), 2) AS order_share_pct,
    
    ROUND(pmm.total_revenue * 100.0 / SUM(pmm.total_revenue) OVER (), 2) AS revenue_share_pct,
    
    DENSE_RANK() OVER (ORDER BY pmm.total_revenue DESC) AS revenue_rank
    
  FROM payment_method_metrics pmm
),

final_summary AS (
  SELECT
    pms.payment_method_primary,
    pms.total_orders,
    pms.successful_orders,
    pms.canceled_orders,
    pms.total_revenue,
    pms.avg_order_value,
    pms.min_order_value,
    pms.max_order_value,
    pms.avg_installments_per_method,
    pms.max_installments_per_method,
    pms.conversion_rate,
    pms.cancellation_rate,
    pms.order_share_pct,
    pms.revenue_share_pct,
    pms.revenue_rank,
    pms.orders_with_multiple_methods,
    ROUND(pms.orders_with_multiple_methods * 100.0 / pms.total_orders, 2) AS pct_multiple_methods
    
  FROM payment_method_share pms
)

SELECT *
FROM final_summary
ORDER BY revenue_rank;


/*
-- VALIDATION QUERIES
-- 1. Payment method distribution (orders and revenue)
SELECT
  payment_method_primary,
  total_orders,
  order_share_pct,
  total_revenue,
  revenue_share_pct,
  avg_order_value
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_analysis`
ORDER BY revenue_share_pct DESC;

-- 2. Conversion and cancellation rates by method
SELECT
  payment_method_primary,
  total_orders,
  successful_orders,
  canceled_orders,
  conversion_rate,
  cancellation_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_analysis`
ORDER BY conversion_rate DESC;

-- 3. AOV by payment method
SELECT
  payment_method_primary,
  avg_order_value,
  min_order_value,
  max_order_value,
  total_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_analysis`
ORDER BY avg_order_value DESC;

-- 4. Installments analysis by method
SELECT
  payment_method_primary,
  avg_installments_per_method,
  max_installments_per_method,
  avg_order_value
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_analysis`
ORDER BY avg_installments_per_method DESC;

-- 5. Multiple payment methods usage
SELECT
  payment_method_primary,
  total_orders,
  orders_with_multiple_methods,
  pct_multiple_methods
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_analysis`
ORDER BY pct_multiple_methods DESC;

-- 6. Installment bin analysis (requires separate query on raw data)
WITH installment_bins AS (
  SELECT
    CASE
      WHEN p.payment_installments = 1 THEN '1x'
      WHEN p.payment_installments BETWEEN 2 AND 3 THEN '2-3x'
      WHEN p.payment_installments BETWEEN 4 AND 6 THEN '4-6x'
      WHEN p.payment_installments BETWEEN 7 AND 12 THEN '7-12x'
      WHEN p.payment_installments >= 13 THEN '13+x'
    END AS installment_bin,
    o.order_id,
    p.payment_value,
    o.is_delivered,
    o.is_completed
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
    ON p.order_id = o.order_id
)
SELECT
  installment_bin,
  COUNT(DISTINCT order_id) AS total_orders,
  ROUND(COUNT(DISTINCT order_id) * 100.0 / SUM(COUNT(DISTINCT order_id)) OVER (), 2) AS order_pct,
  ROUND(AVG(payment_value), 2) AS avg_payment_value,
  ROUND(SUM(payment_value), 2) AS total_revenue,
  ROUND(COUNTIF(is_delivered OR is_completed) * 100.0 / COUNT(*), 2) AS conversion_rate
FROM installment_bins
WHERE installment_bin IS NOT NULL
GROUP BY installment_bin
ORDER BY 
  CASE installment_bin
    WHEN '1x' THEN 1
    WHEN '2-3x' THEN 2
    WHEN '4-6x' THEN 3
    WHEN '7-12x' THEN 4
    WHEN '13+x' THEN 5
  END;

-- 7. Payment method performance summary
SELECT
  payment_method_primary,
  total_orders,
  order_share_pct,
  revenue_share_pct,
  avg_order_value,
  conversion_rate,
  cancellation_rate,
  avg_installments_per_method,
  CASE
    WHEN conversion_rate >= 95 AND avg_order_value >= (SELECT AVG(avg_order_value) FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_analysis`) THEN 'Excellent'
    WHEN conversion_rate >= 90 OR avg_order_value >= (SELECT AVG(avg_order_value) FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_analysis`) THEN 'Good'
    WHEN conversion_rate >= 80 THEN 'Average'
    ELSE 'Needs Improvement'
  END AS performance_category
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_analysis`
ORDER BY revenue_rank;

-- 8. Revenue concentration by payment method (Pareto)
SELECT
  payment_method_primary,
  revenue_share_pct,
  SUM(revenue_share_pct) OVER (ORDER BY revenue_share_pct DESC) AS cumulative_revenue_pct
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_analysis`
ORDER BY revenue_share_pct DESC;
*/