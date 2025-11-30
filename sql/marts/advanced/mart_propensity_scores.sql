-- =========================================================
-- MART: PROPENSITY SCORES
-- =========================================================
-- Description: Customer propensity scores for churn, category affinity, and loyalty
-- Sources: All staging tables + customer marts
-- Destination: olist_marts.mart_propensity_scores
-- Granularity: 1 row per customer with propensity scores
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_propensity_scores` AS

WITH first_purchase_per_customer AS (

  SELECT
    o.customer_id,
    MIN(o.order_purchase_timestamp) AS first_purchase_timestamp
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  WHERE o.is_completed = TRUE
  GROUP BY o.customer_id
),

customer_base_features AS (

  SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_state,
    c.customer_region,
    
    
    DATE_DIFF(CURRENT_DATE(), DATE(MAX(o.order_purchase_timestamp)), DAY) AS recency_days,
    COUNT(DISTINCT o.order_id) AS frequency,
    SUM(p.payment_value) AS monetary,
    
    DATE_DIFF(CURRENT_DATE(), DATE(MIN(o.order_purchase_timestamp)), DAY) AS customer_lifetime_days,
    
    DATE_DIFF(CURRENT_DATE(), DATE(MAX(o.order_purchase_timestamp)), DAY) AS days_since_last_purchase,
    
    MIN(o.order_purchase_timestamp) AS first_purchase_date,
    
    SUM(
      CASE 
        WHEN o.order_purchase_timestamp = fp.first_purchase_timestamp 
        THEN p.payment_value 
        ELSE 0 
      END
    ) AS first_purchase_value,
    
    AVG(r.review_score) AS avg_review_score,
    COUNT(r.review_id) AS total_reviews,
    
    APPROX_TOP_COUNT(p.payment_type, 1)[OFFSET(0)].value AS preferred_payment_method,
    AVG(p.payment_installments) AS avg_installments,
    SUM(CASE WHEN p.payment_type = 'credit_card' THEN 1 ELSE 0 END) AS credit_card_payments,
    SUM(CASE WHEN p.payment_type = 'boleto' THEN 1 ELSE 0 END) AS boleto_payments
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_customers` c
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o 
    ON c.customer_id = o.customer_id
  INNER JOIN first_purchase_per_customer fp
    ON o.customer_id = fp.customer_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p 
    ON o.order_id = p.order_id
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_reviews` r 
    ON o.order_id = r.order_id
  WHERE o.is_completed = TRUE
  GROUP BY c.customer_id, c.customer_unique_id, c.customer_state, c.customer_region
),

purchase_behavior_base AS (
  SELECT
    o.customer_id,
    o.order_id,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_purchase_timestamp) AS order_sequence,
    DATE_DIFF(
      o.order_purchase_timestamp,
      MIN(o.order_purchase_timestamp) OVER (PARTITION BY o.customer_id),
      DAY
    ) AS days_since_first_order,
    LAG(o.order_purchase_timestamp) OVER (
      PARTITION BY o.customer_id 
      ORDER BY o.order_purchase_timestamp
    ) AS previous_order_timestamp
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  WHERE o.is_completed = TRUE
    AND o.order_delivered_customer_date IS NOT NULL
),

purchase_behavior AS (
  SELECT
    customer_id,
    
    AVG(DATE_DIFF(order_purchase_timestamp, previous_order_timestamp, DAY)) AS avg_days_between_purchases,
    
    CASE 
      WHEN COUNT(order_id) >= 3 THEN
        CORR(order_sequence, days_since_first_order)
      ELSE 0 
    END AS purchase_trend_correlation,
    
    AVG(DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY)) AS avg_delivery_days,
    SUM(CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 1 ELSE 0 END) AS delayed_orders,
    COUNT(order_id) AS total_orders_for_metrics
    
  FROM purchase_behavior_base
  GROUP BY customer_id
),

category_affinity AS (
  SELECT
    o.customer_id,
    
    COUNT(DISTINCT p.product_category_name_english) AS unique_categories_purchased,
    ARRAY_AGG(DISTINCT p.product_category_name_english IGNORE NULLS) AS purchased_categories,
    
    APPROX_TOP_COUNT(p.product_category_name_english, 1)[OFFSET(0)].value AS top_category,
    
    SUM(POW(category_count.total_orders / customer_orders.total_orders, 2)) AS category_concentration_index
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi 
    ON o.order_id = oi.order_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` p 
    ON oi.product_id = p.product_id
  INNER JOIN (
    SELECT customer_id, COUNT(DISTINCT order_id) AS total_orders
    FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders`
    WHERE is_completed = TRUE
    GROUP BY customer_id
  ) customer_orders ON o.customer_id = customer_orders.customer_id
  INNER JOIN (
    SELECT o.customer_id, p.product_category_name_english, COUNT(DISTINCT o.order_id) AS total_orders
    FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
    INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi ON o.order_id = oi.order_id
    INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` p ON oi.product_id = p.product_id
    WHERE o.is_completed = TRUE
    GROUP BY o.customer_id, p.product_category_name_english
  ) category_count ON o.customer_id = category_count.customer_id
  WHERE o.is_completed = TRUE
  GROUP BY o.customer_id
),

first_order_details AS (
  SELECT
    o.customer_id,
  
    FIRST_VALUE(prod.product_category_name_english) OVER (
      PARTITION BY o.customer_id 
      ORDER BY o.order_purchase_timestamp
    ) AS first_purchase_category,
    
    FIRST_VALUE(DATE_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY)) OVER (
      PARTITION BY o.customer_id 
      ORDER BY o.order_purchase_timestamp
    ) AS first_order_delivery_days,
    
    FIRST_VALUE(r.review_score) OVER (
      PARTITION BY o.customer_id 
      ORDER BY o.order_purchase_timestamp
    ) AS first_order_review_score,
    
    FIRST_VALUE(pay.payment_type) OVER (
      PARTITION BY o.customer_id 
      ORDER BY o.order_purchase_timestamp
    ) AS first_payment_method
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi 
    ON o.order_id = oi.order_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` prod 
    ON oi.product_id = prod.product_id
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` pay 
    ON o.order_id = pay.order_id
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_reviews` r 
    ON o.order_id = r.order_id
  WHERE o.is_completed = TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_purchase_timestamp) = 1
),


-- PROPENSITY SCORES (ML)

final_propensity_scores AS (
  SELECT
    -- Customer Identification
    cbf.customer_id,
    cbf.customer_unique_id,
    cbf.customer_state,
    cbf.customer_region,
    
    -- Base Features
    cbf.recency_days,
    cbf.frequency,
    cbf.monetary,
    cbf.customer_lifetime_days,
    cbf.days_since_last_purchase,
    cbf.avg_review_score,
    cbf.total_reviews,
    cbf.preferred_payment_method,
    cbf.avg_installments,
    
    -- Behavioral Features
    pb.avg_days_between_purchases,
    pb.purchase_trend_correlation,
    pb.avg_delivery_days,
    pb.delayed_orders,
    pb.total_orders_for_metrics,
    
    -- Category Features
    ca.unique_categories_purchased,
    ca.purchased_categories,
    ca.top_category,
    ca.category_concentration_index,
    
    -- First Order Features (for loyalty model)
    fod.first_purchase_category,
    fod.first_order_delivery_days,
    fod.first_order_review_score,
    fod.first_payment_method,
    cbf.first_purchase_value,
    
   
    -- PROPENSITY SCORES (ML)
    -- A) Propensity to Churn
    CAST(NULL AS FLOAT64) AS churn_probability,
    CASE 
      WHEN CAST(NULL AS FLOAT64) BETWEEN 0 AND 0.3 THEN 'Low'
      WHEN CAST(NULL AS FLOAT64) BETWEEN 0.3 AND 0.7 THEN 'Medium' 
      WHEN CAST(NULL AS FLOAT64) > 0.7 THEN 'High'
      ELSE 'Unknown'
    END AS churn_risk_segment,
    
    -- B) Propensity to Buy Category (placeholder for top category)
    CAST(NULL AS FLOAT64) AS top_category_propensity_score,
    ca.top_category AS next_best_category,
    
    -- C) Propensity to Become Loyal  
    CAST(NULL AS FLOAT64) AS loyalty_probability,
    CASE 
      WHEN cbf.frequency = 1 THEN 'One-time Customer'
      WHEN cbf.frequency > 1 THEN 'Repeat Customer'
      ELSE 'Unknown'
    END AS current_customer_status,
    
    -- Campaign timing suggestion (placeholder)
    CASE 
      WHEN CAST(NULL AS FLOAT64) BETWEEN 0 AND 0.3 THEN 'No immediate action'
      WHEN CAST(NULL AS FLOAT64) BETWEEN 0.3 AND 0.7 THEN 'Contact in 30 days'
      WHEN CAST(NULL AS FLOAT64) > 0.7 THEN 'Contact immediately'
      ELSE 'Monitor'
    END AS campaign_timing,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS score_calculation_date,
    'ML_MODEL_PLACEHOLDER' AS model_version
    
  FROM customer_base_features cbf
  LEFT JOIN purchase_behavior pb ON cbf.customer_id = pb.customer_id
  LEFT JOIN category_affinity ca ON cbf.customer_id = ca.customer_id
  LEFT JOIN first_order_details fod ON cbf.customer_id = fod.customer_id
  WHERE cbf.frequency >= 1
)

SELECT *
FROM final_propensity_scores
ORDER BY churn_probability DESC NULLS LAST, loyalty_probability DESC NULLS LAST;


/*
-- VALIDATION QUERIES
-- 1. Check feature completeness
SELECT 
  COUNT(*) AS total_customers,
  COUNTIF(recency_days IS NOT NULL) AS has_recency,
  COUNTIF(frequency IS NOT NULL) AS has_frequency,
  COUNTIF(monetary IS NOT NULL) AS has_monetary,
  COUNTIF(avg_review_score IS NOT NULL) AS has_reviews,
  COUNTIF(top_category IS NOT NULL) AS has_category_affinity
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_propensity_scores`;

-- 2. Feature distribution summary
SELECT 
  ROUND(AVG(recency_days), 1) AS avg_recency,
  ROUND(AVG(frequency), 1) AS avg_frequency,
  ROUND(AVG(monetary), 2) AS avg_monetary,
  ROUND(AVG(avg_review_score), 2) AS avg_review,
  ROUND(AVG(unique_categories_purchased), 1) AS avg_category_diversity
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_propensity_scores`;

-- 3. Customer status distribution
SELECT 
  current_customer_status,
  COUNT(*) AS customers,
  ROUND(AVG(monetary), 2) AS avg_ltv,
  ROUND(AVG(frequency), 1) AS avg_orders
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_propensity_scores`
GROUP BY current_customer_status;

-- 4. Top categories by customer count
SELECT 
  top_category,
  COUNT(*) AS customers,
  ROUND(AVG(monetary), 2) AS avg_ltv
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_propensity_scores`
WHERE top_category IS NOT NULL
GROUP BY top_category
ORDER BY customers DESC
LIMIT 10;

-- 5. First order impact analysis
SELECT 
  first_payment_method,
  COUNT(*) AS customers,
  ROUND(AVG(CASE WHEN frequency > 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS repeat_rate,
  ROUND(AVG(first_order_review_score), 2) AS avg_first_review
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_propensity_scores`
WHERE first_payment_method IS NOT NULL
GROUP BY first_payment_method
ORDER BY customers DESC;
*/