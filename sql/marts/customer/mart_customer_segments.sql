-- =========================================================
-- MART: CUSTOMER SEGMENTS
-- =========================================================
-- Description: Advanced customer segmentation combining behavioral features
-- Based on: Analysis #7 (Customer Segmentation - Clustering)
-- Source: mart_customer_base, mart_customer_rfm, mart_customer_ltv, stg_orders, stg_order_items, stg_products
-- Destination: olist_marts.mart_customer_segments
-- Note: This creates feature-rich customer profiles for segmentation
-- Actual clustering (K-means) should be done in Python/ML pipeline
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_customer_segments` AS

WITH customer_purchase_timing AS (

  SELECT
    o.customer_id,
    
    COUNTIF(EXTRACT(DAYOFWEEK FROM o.order_purchase_timestamp) IN (1, 7)) AS weekend_orders,
    COUNT(*) AS total_orders_timing,
    ROUND(COUNTIF(EXTRACT(DAYOFWEEK FROM o.order_purchase_timestamp) IN (1, 7)) * 100.0 / COUNT(*), 2) AS pct_weekend_shopper,
    
    
    COUNTIF(EXTRACT(HOUR FROM o.order_purchase_timestamp) >= 20 OR EXTRACT(HOUR FROM o.order_purchase_timestamp) < 6) AS night_orders,
    ROUND(COUNTIF(EXTRACT(HOUR FROM o.order_purchase_timestamp) >= 20 OR EXTRACT(HOUR FROM o.order_purchase_timestamp) < 6) * 100.0 / COUNT(*), 2) AS pct_night_shopper
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  WHERE o.is_delivered OR o.is_completed
  GROUP BY o.customer_id
),

customer_category_preferences AS (
  
  SELECT
    o.customer_id,
    
    ARRAY_AGG(p.product_category_name_english ORDER BY item_count DESC LIMIT 1)[OFFSET(0)] AS preferred_category,
    
  
    COUNT(DISTINCT p.product_category_name_english) AS category_diversity,
    

    SUM(item_count) AS total_items_purchased
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN (
    SELECT
      oi.order_id,
      p.product_category_name_english,
      COUNT(*) AS item_count
    FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
    INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` p
      ON oi.product_id = p.product_id
    GROUP BY oi.order_id, p.product_category_name_english
  ) p ON o.order_id = p.order_id
  WHERE o.is_delivered OR o.is_completed
  GROUP BY o.customer_id
),

customer_payment_behavior AS (
  
  SELECT
    o.customer_id,
    
    ARRAY_AGG(p.payment_type ORDER BY payment_count DESC LIMIT 1)[OFFSET(0)] AS preferred_payment_method,
    
    ROUND(AVG(p.payment_installments), 2) AS avg_installments
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN (
    SELECT
      order_id,
      payment_type,
      AVG(payment_installments) AS payment_installments,
      COUNT(*) AS payment_count
    FROM `quintoandar-ecommerce-analysis.olist_staging.stg_payments`
    GROUP BY order_id, payment_type
  ) p ON o.order_id = p.order_id
  WHERE o.is_delivered OR o.is_completed
  GROUP BY o.customer_id
),

customer_delivery_experience AS (
  SELECT
    customer_id,
    ROUND(AVG(CASE WHEN is_delayed THEN 1.0 ELSE 0.0 END) * 100, 2) AS pct_delayed_orders
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance`
  GROUP BY customer_id
),

customer_feature_base AS (
  
  SELECT
    cb.customer_id,
    cb.customer_state,
    cb.customer_region,
    
    
    cb.total_orders AS frequency,
    cb.total_revenue AS monetary,
    cb.avg_order_value,
    cb.total_items_purchased,
    cb.avg_item_price,
    cb.total_freight_paid,
    cb.avg_review_score,
    cb.customer_lifespan_days,
    
    rfm.recency,
    rfm.r_score,
    rfm.f_score,
    rfm.m_score,
    rfm.rfm_segment,
    
    ltv.clv_segment,
    ltv.orders_per_month,
    
    ROUND(CASE 
      WHEN cb.total_orders > 1 
      THEN cb.customer_lifespan_days / NULLIF(cb.total_orders - 1, 0)
      ELSE NULL
    END, 2) AS avg_days_between_purchases,
    
    ROUND(cb.total_items_purchased / NULLIF(cb.total_orders, 0), 2) AS avg_items_per_order
    
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_base` cb
  INNER JOIN `quintoandar-ecommerce-analysis.olist_marts.mart_customer_rfm` rfm
    ON cb.customer_id = rfm.customer_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_marts.mart_customer_ltv` ltv
    ON cb.customer_id = ltv.customer_id
),

customer_segments_enriched AS (
  
  SELECT
    cfb.*,
    
    COALESCE(cpt.pct_weekend_shopper, 0) AS pct_weekend_shopper,
    COALESCE(cpt.pct_night_shopper, 0) AS pct_night_shopper,
    
    COALESCE(ccp.preferred_category, 'unknown') AS preferred_category,
    COALESCE(ccp.category_diversity, 0) AS category_diversity,
  
    COALESCE(cpb.preferred_payment_method, 'unknown') AS preferred_payment_method,
    COALESCE(cpb.avg_installments, 0) AS avg_installments,
    
    COALESCE(cde.pct_delayed_orders, 0) AS pct_delayed_orders,
    
    CASE

      WHEN cfb.rfm_segment IN ('Champions', 'Loyal Customers') 
           AND cfb.monetary >= PERCENTILE_CONT(cfb.monetary, 0.75) OVER ()
           THEN 'High Value Regulars'
      
      WHEN cfb.avg_order_value >= PERCENTILE_CONT(cfb.avg_order_value, 0.80) OVER ()
           AND cfb.frequency >= 2
           THEN 'Premium Shoppers'
      
      WHEN COALESCE(cpb.avg_installments, 0) >= 6
           AND cfb.avg_order_value < PERCENTILE_CONT(cfb.avg_order_value, 0.50) OVER ()
           THEN 'Bargain Hunters'
      
      WHEN COALESCE(ccp.category_diversity, 0) <= 2
           AND cfb.frequency >= 3
           THEN 'Category Focused'
      
      WHEN cfb.frequency = 1
           AND cfb.recency <= 180
           THEN 'Occasional Shoppers'
      
      WHEN COALESCE(cpt.pct_weekend_shopper, 0) >= 60
           THEN 'Weekend Warriors'
      
      WHEN COALESCE(cpt.pct_night_shopper, 0) >= 50
           THEN 'Night Owls'
      
      WHEN cfb.rfm_segment IN ('At Risk', "Can't Lose Them")
           AND cfb.monetary >= PERCENTILE_CONT(cfb.monetary, 0.70) OVER ()
           THEN 'At Risk High Value'
      
      WHEN cfb.rfm_segment IN ('Hibernating', 'Lost')
           THEN 'Dormant'
      
      ELSE 'Standard Shoppers'
    END AS segment_name
    
  FROM customer_feature_base cfb
  LEFT JOIN customer_purchase_timing cpt
    ON cfb.customer_id = cpt.customer_id
  LEFT JOIN customer_category_preferences ccp
    ON cfb.customer_id = ccp.customer_id
  LEFT JOIN customer_payment_behavior cpb
    ON cfb.customer_id = cpb.customer_id
  LEFT JOIN customer_delivery_experience cde
    ON cfb.customer_id = cde.customer_id
)

SELECT
  customer_id,
  customer_state,
  customer_region,
  segment_name,
  rfm_segment,
  clv_segment,
  
  -- RFM features
  recency,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  
  -- Behavioral features
  avg_order_value,
  avg_items_per_order,
  avg_days_between_purchases,
  orders_per_month,
  customer_lifespan_days,
  
  -- Category features
  preferred_category,
  category_diversity,
  
  -- Payment features
  preferred_payment_method,
  avg_installments,
  
  -- Shopping pattern features
  pct_weekend_shopper,
  pct_night_shopper,
  
  -- Experience features
  avg_review_score,
  pct_delayed_orders,
  total_freight_paid
  
FROM customer_segments_enriched
ORDER BY monetary DESC;


/*
-- VALIDATION QUERIES
-- 1. Segment distribution
SELECT
  segment_name,
  COUNT(*) AS num_customers,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_customers
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_segments`
GROUP BY segment_name
ORDER BY num_customers DESC;

-- 2. Segment profile (average characteristics)
SELECT
  segment_name,
  COUNT(*) AS num_customers,
  ROUND(AVG(monetary), 2) AS avg_ltv,
  ROUND(AVG(avg_order_value), 2) AS avg_aov,
  ROUND(AVG(frequency), 1) AS avg_frequency,
  ROUND(AVG(recency), 0) AS avg_recency_days,
  ROUND(AVG(category_diversity), 1) AS avg_category_diversity,
  ROUND(AVG(avg_review_score), 2) AS avg_review_score
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_segments`
GROUP BY segment_name
ORDER BY avg_ltv DESC;

-- 3. Revenue by segment
SELECT
  segment_name,
  COUNT(*) AS customers,
  ROUND(SUM(monetary), 2) AS total_revenue,
  ROUND(SUM(monetary) * 100.0 / SUM(SUM(monetary)) OVER (), 2) AS pct_revenue,
  ROUND(AVG(monetary), 2) AS avg_ltv
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_segments`
GROUP BY segment_name
ORDER BY total_revenue DESC;

-- 4. Segment characteristics - category preferences
SELECT
  segment_name,
  preferred_category,
  COUNT(*) AS num_customers,
  ROUND(AVG(category_diversity), 1) AS avg_category_diversity
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_segments`
GROUP BY segment_name, preferred_category
QUALIFY ROW_NUMBER() OVER (PARTITION BY segment_name ORDER BY COUNT(*) DESC) <= 3
ORDER BY segment_name, num_customers DESC;

-- 5. Segment characteristics - payment behavior
SELECT
  segment_name,
  preferred_payment_method,
  COUNT(*) AS num_customers,
  ROUND(AVG(avg_installments), 1) AS avg_installments
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_segments`
GROUP BY segment_name, preferred_payment_method
QUALIFY ROW_NUMBER() OVER (PARTITION BY segment_name ORDER BY COUNT(*) DESC) = 1
ORDER BY segment_name;

-- 6. Shopping patterns by segment
SELECT
  segment_name,
  ROUND(AVG(pct_weekend_shopper), 1) AS avg_pct_weekend,
  ROUND(AVG(pct_night_shopper), 1) AS avg_pct_night,
  ROUND(AVG(avg_days_between_purchases), 0) AS avg_days_between_purchases
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_segments`
WHERE avg_days_between_purchases IS NOT NULL
GROUP BY segment_name
ORDER BY avg_pct_weekend DESC;

-- 7. Experience metrics by segment
SELECT
  segment_name,
  ROUND(AVG(avg_review_score), 2) AS avg_review_score,
  ROUND(AVG(pct_delayed_orders), 1) AS avg_pct_delayed,
  ROUND(AVG(total_freight_paid), 2) AS avg_freight_paid
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_segments`
GROUP BY segment_name
ORDER BY avg_review_score DESC;

-- 8. Geographic distribution by segment
SELECT
  segment_name,
  customer_region,
  COUNT(*) AS num_customers,
  ROUND(SUM(monetary), 2) AS total_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_segments`
GROUP BY segment_name, customer_region
ORDER BY segment_name, total_revenue DESC;

-- 9. Segment overlap with RFM
SELECT
  segment_name,
  rfm_segment,
  COUNT(*) AS num_customers,
  ROUND(AVG(monetary), 2) AS avg_ltv
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_segments`
GROUP BY segment_name, rfm_segment
ORDER BY segment_name, num_customers DESC;

-- 10. Feature importance proxy (variability by segment)
SELECT
  segment_name,
  ROUND(STDDEV(monetary), 2) AS stddev_ltv,
  ROUND(STDDEV(avg_order_value), 2) AS stddev_aov,
  ROUND(STDDEV(frequency), 2) AS stddev_frequency,
  ROUND(STDDEV(category_diversity), 2) AS stddev_category_diversity
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_segments`
GROUP BY segment_name
ORDER BY segment_name;
*/