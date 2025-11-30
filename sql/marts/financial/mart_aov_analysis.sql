-- =========================================================
-- MART: AOV ANALYSIS
-- =========================================================
-- Description: Average Order Value analysis with segmentation and trends
-- Sources: stg_orders, stg_payments, stg_order_items, stg_customers
-- Destination: olist_marts.mart_aov_analysis
-- Granularity: 1 row per order with segmentations
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_aov_analysis` AS

WITH order_value_base AS (
  
  SELECT
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_status,
    o.is_completed,
    
    SUM(p.payment_value) AS order_value,
    COUNT(DISTINCT oi.order_item_id) AS items_per_order,
    COUNT(DISTINCT oi.product_id) AS unique_products_per_order,
    SUM(oi.freight_value) AS total_freight_value,
    
    CASE 
      WHEN COUNT(DISTINCT oi.order_item_id) > 0 
      THEN SUM(p.payment_value) / COUNT(DISTINCT oi.order_item_id) 
      ELSE 0 
    END AS avg_item_price,
    
    CASE 
      WHEN COUNT(DISTINCT oi.order_item_id) > 0 
      THEN SUM(oi.freight_value) / COUNT(DISTINCT oi.order_item_id) 
      ELSE 0 
    END AS avg_freight_per_item,
    
    ARRAY_AGG(DISTINCT p.payment_type IGNORE NULLS) AS payment_methods_used,
    MAX(p.payment_installments) AS max_installments,
    SUM(CASE WHEN p.is_credit_card THEN 1 ELSE 0 END) AS credit_card_payments,
    SUM(CASE WHEN p.is_boleto THEN 1 ELSE 0 END) AS boleto_payments
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
    ON o.order_id = oi.order_id
  WHERE o.is_completed = TRUE
  GROUP BY 
    o.order_id, o.customer_id, o.order_purchase_timestamp, 
    o.order_status, o.is_completed
),

order_segmentation AS (
  
  SELECT
    ovb.*,
    c.customer_state,
    c.customer_city,
    
    
    CASE
      WHEN ovb.order_value < 50 THEN 'low'
      WHEN ovb.order_value BETWEEN 50 AND 150 THEN 'medium'
      WHEN ovb.order_value BETWEEN 150 AND 300 THEN 'high'
      ELSE 'premium'
    END AS value_bin,
    
    
    CASE
      WHEN ovb.items_per_order = 1 THEN 'single_item'
      WHEN ovb.items_per_order BETWEEN 2 AND 3 THEN 'few_items'
      WHEN ovb.items_per_order BETWEEN 4 AND 6 THEN 'multiple_items'
      ELSE 'many_items'
    END AS items_bin,
    
    
    CASE
      WHEN ovb.order_value > 0 THEN
        ovb.total_freight_value / ovb.order_value
      ELSE 0
    END AS freight_to_value_ratio,
    
    
    DATE_TRUNC(ovb.order_purchase_timestamp, MONTH) AS order_month,
    DATE_TRUNC(ovb.order_purchase_timestamp, WEEK) AS order_week,
    EXTRACT(YEAR FROM ovb.order_purchase_timestamp) AS order_year,
    EXTRACT(QUARTER FROM ovb.order_purchase_timestamp) AS order_quarter,
    
    
    CASE
      WHEN ARRAY_LENGTH(ovb.payment_methods_used) > 1 THEN 'mixed'
      WHEN ovb.payment_methods_used[OFFSET(0)] = 'credit_card' THEN 'credit_card_only'
      WHEN ovb.payment_methods_used[OFFSET(0)] = 'boleto' THEN 'boleto_only'
      WHEN ovb.payment_methods_used[OFFSET(0)] = 'debit_card' THEN 'debit_card_only'
      WHEN ovb.payment_methods_used[OFFSET(0)] = 'voucher' THEN 'voucher_only'
      ELSE 'other'
    END AS payment_method_segment,
    
    
    CASE
      WHEN ovb.max_installments = 1 THEN 'no_installments'
      WHEN ovb.max_installments BETWEEN 2 AND 3 THEN 'few_installments'
      WHEN ovb.max_installments BETWEEN 4 AND 6 THEN 'medium_installments'
      ELSE 'many_installments'
    END AS installments_segment

  FROM order_value_base ovb
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_customers` c
    ON ovb.customer_id = c.customer_id
  WHERE ovb.order_value > 0  
),

final_aov_analysis AS (
  
  SELECT
    order_id,
    customer_id,
    customer_state,
    customer_city,
    
    
    order_value,
    items_per_order,
    unique_products_per_order,
    total_freight_value,
    avg_item_price,
    avg_freight_per_item,
    freight_to_value_ratio,
    
    
    value_bin,
    items_bin,
    payment_method_segment,
    installments_segment,
    
    
    order_purchase_timestamp,
    order_month,
    order_week,
    order_year,
    order_quarter,
    
  
    payment_methods_used,
    max_installments,
    credit_card_payments,
    boleto_payments,
    
    
    CASE 
      WHEN order_value > 1000 THEN 1 
      ELSE 0 
    END AS is_high_value_order,
    
    CASE 
      WHEN items_per_order > 5 THEN 1 
      ELSE 0 
    END AS is_high_item_count_order,
    
    CASE 
      WHEN freight_to_value_ratio > 0.3 THEN 1 
      ELSE 0 
    END AS has_high_freight_ratio,
    
  
    ROW_NUMBER() OVER (
      PARTITION BY customer_id 
      ORDER BY order_purchase_timestamp
    ) AS customer_order_sequence

  FROM order_segmentation
)

SELECT *
FROM final_aov_analysis
ORDER BY order_purchase_timestamp DESC;


-- Uncomment and run separately to validate the mart:
/*
-- VALIDATION QUERIES
-- 1. Overall AOV and key metrics
SELECT 
  COUNT(*) AS total_orders,
  ROUND(AVG(order_value), 2) AS overall_aov,
  ROUND(AVG(items_per_order), 2) AS avg_items_per_order,
  ROUND(AVG(avg_item_price), 2) AS avg_item_price,
  ROUND(MIN(order_value), 2) AS min_order_value,
  ROUND(MAX(order_value), 2) AS max_order_value
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_aov_analysis`;

-- 2. AOV distribution by value bins
SELECT 
  value_bin,
  COUNT(*) AS order_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
  ROUND(AVG(order_value), 2) AS avg_order_value,
  ROUND(AVG(items_per_order), 2) AS avg_items_per_order
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_aov_analysis`
GROUP BY value_bin
ORDER BY 
  CASE value_bin
    WHEN 'low' THEN 1
    WHEN 'medium' THEN 2
    WHEN 'high' THEN 3
    WHEN 'premium' THEN 4
  END;

-- 3. AOV by payment method segment
SELECT 
  payment_method_segment,
  COUNT(*) AS order_count,
  ROUND(AVG(order_value), 2) AS avg_order_value,
  ROUND(AVG(items_per_order), 2) AS avg_items_per_order,
  ROUND(AVG(max_installments), 2) AS avg_installments
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_aov_analysis`
GROUP BY payment_method_segment
ORDER BY avg_order_value DESC;

-- 4. AOV by items bin segmentation
SELECT 
  items_bin,
  COUNT(*) AS order_count,
  ROUND(AVG(order_value), 2) AS avg_order_value,
  ROUND(AVG(avg_item_price), 2) AS avg_item_price,
  ROUND(AVG(freight_to_value_ratio), 4) AS avg_freight_ratio
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_aov_analysis`
GROUP BY items_bin
ORDER BY 
  CASE items_bin
    WHEN 'single_item' THEN 1
    WHEN 'few_items' THEN 2
    WHEN 'multiple_items' THEN 3
    WHEN 'many_items' THEN 4
  END;

-- 5. Monthly AOV trends
SELECT 
  order_month,
  COUNT(*) AS order_count,
  ROUND(AVG(order_value), 2) AS monthly_aov,
  ROUND(AVG(items_per_order), 2) AS avg_items_per_order,
  ROUND(SUM(order_value), 2) AS total_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_aov_analysis`
GROUP BY order_month
ORDER BY order_month;

-- 6. AOV by customer state (top 10)
SELECT 
  customer_state,
  COUNT(*) AS order_count,
  ROUND(AVG(order_value), 2) AS avg_order_value,
  ROUND(AVG(items_per_order), 2) AS avg_items_per_order
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_aov_analysis`
WHERE customer_state IS NOT NULL
GROUP BY customer_state
ORDER BY avg_order_value DESC
LIMIT 10;

-- 7. Correlation analysis: order value vs items per order
SELECT 
  CORR(order_value, items_per_order) AS value_items_correlation,
  CORR(order_value, avg_item_price) AS value_price_correlation,
  CORR(items_per_order, avg_item_price) AS items_price_correlation
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_aov_analysis`;

-- 8. High value order analysis
SELECT 
  is_high_value_order,
  COUNT(*) AS order_count,
  ROUND(AVG(order_value), 2) AS avg_order_value,
  ROUND(AVG(items_per_order), 2) AS avg_items_per_order,
  ROUND(AVG(max_installments), 2) AS avg_installments
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_aov_analysis`
GROUP BY is_high_value_order;
*/