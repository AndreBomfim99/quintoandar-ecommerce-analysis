/*
=============================================================================
TABLE: olist_marts.mart_churn_prediction
DESCRIPTION: Customer churn prediction features and risk scoring
ANALYSIS: #3 - Churn Prediction

TRANSFORMATIONS:
- Calculate recency features (days since last purchase)
- Compute frequency and monetary metrics
- Calculate behavioral features (payment, delivery, reviews)
- Define churn target (180 days or 2x avg purchase interval)
- Normalize features to 0-100 scale for ML readiness
- Calculate churn risk score

BUSINESS VALUE:
- Identify at-risk customers for retention campaigns
- Prioritize customer reactivation efforts
- Understand churn drivers and patterns
- Optimize marketing spend on retention
- Predict future churn probability

ROW COUNT: ~96,000 rows (one per customer)
=============================================================================
*/

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction` AS

WITH customer_order_history AS (
  
  SELECT 
    o.customer_id,
    o.order_id,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.is_completed,
    o.is_delivered,
    o.is_canceled,
    p.payment_value,
    p.payment_type,
    p.payment_installments,
    r.review_score,
    i.freight_value,
    i.price,
    prod.product_category_name_english,
   
    CASE 
      WHEN o.order_delivered_customer_date IS NOT NULL 
           AND o.order_estimated_delivery_date IS NOT NULL 
      THEN DATE_DIFF(
        DATE(o.order_delivered_customer_date),
        DATE(o.order_estimated_delivery_date),
        DAY
      )
      ELSE 0
    END AS delivery_delay_days
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_reviews` r
    ON o.order_id = r.order_id
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` i
    ON o.order_id = i.order_id
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` prod
    ON i.product_id = prod.product_id
  WHERE o.is_completed = TRUE
),

customer_purchase_intervals AS (
  
  SELECT 
    customer_id,
    order_purchase_timestamp,
    LAG(order_purchase_timestamp) OVER (
      PARTITION BY customer_id 
      ORDER BY order_purchase_timestamp
    ) AS previous_purchase_timestamp,
    DATE_DIFF(
      DATE(order_purchase_timestamp),
      DATE(LAG(order_purchase_timestamp) OVER (
        PARTITION BY customer_id 
        ORDER BY order_purchase_timestamp
      )),
      DAY
    ) AS days_between_purchases
  FROM customer_order_history
),

customer_recency_frequency_monetary AS (
  
  SELECT 
    customer_id,
    
    DATE_DIFF(
      CURRENT_DATE(),
      DATE(MAX(order_purchase_timestamp)),
      DAY
    ) AS days_since_last_purchase,
    MIN(DATE(order_purchase_timestamp)) AS first_purchase_date,
    MAX(DATE(order_purchase_timestamp)) AS last_purchase_date,
   
    COUNT(DISTINCT order_id) AS total_orders,
    
    SUM(payment_value) AS total_spent,
    AVG(payment_value) AS avg_order_value
  FROM customer_order_history
  GROUP BY customer_id
),

customer_behavioral_features AS (
 
  SELECT 
    customer_id,
    
    APPROX_TOP_COUNT(payment_type, 1)[OFFSET(0)].value AS preferred_payment_method,
    AVG(payment_installments) AS avg_installments,
    
    AVG(CASE WHEN delivery_delay_days > 0 THEN 1.0 ELSE 0.0 END) AS pct_delayed_orders,
    AVG(delivery_delay_days) AS avg_delivery_delay,
   
    AVG(review_score) AS avg_review_score,
 
    SUM(freight_value) AS total_freight_paid,
    AVG(freight_value) AS avg_freight_per_order,
    
    COUNT(DISTINCT product_category_name_english) AS num_categories_purchased,
    APPROX_TOP_COUNT(product_category_name_english, 1)[OFFSET(0)].value AS preferred_category
  FROM customer_order_history
  GROUP BY customer_id
),

customer_purchase_patterns AS (
  
  SELECT 
    customer_id,
    AVG(days_between_purchases) AS avg_days_between_purchases,
    STDDEV(days_between_purchases) AS stddev_days_between_purchases,
    MIN(days_between_purchases) AS min_days_between_purchases,
    MAX(days_between_purchases) AS max_days_between_purchases
  FROM customer_purchase_intervals
  WHERE days_between_purchases IS NOT NULL
  GROUP BY customer_id
),

customer_features_combined AS (
 
  SELECT 
    rfm.customer_id,
    c.customer_state,
    c.customer_region,
   
    rfm.days_since_last_purchase,
    rfm.first_purchase_date,
    rfm.last_purchase_date,
    DATE_DIFF(rfm.last_purchase_date, rfm.first_purchase_date, DAY) AS customer_lifespan_days,
   
    rfm.total_orders,
   
    rfm.total_spent,
    rfm.avg_order_value,
    
    COALESCE(pp.avg_days_between_purchases, 0) AS avg_days_between_purchases,
    COALESCE(pp.stddev_days_between_purchases, 0) AS stddev_days_between_purchases,
    
    bf.preferred_payment_method,
    bf.avg_installments,
    bf.pct_delayed_orders,
    bf.avg_delivery_delay,
    COALESCE(bf.avg_review_score, 0) AS avg_review_score,
    bf.total_freight_paid,
    bf.avg_freight_per_order,
    bf.num_categories_purchased,
    bf.preferred_category,
   
    rfm_mart.rfm_segment,
    rfm_mart.rfm_score
  FROM customer_recency_frequency_monetary rfm
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_customers` c
    ON rfm.customer_id = c.customer_id
  LEFT JOIN customer_behavioral_features bf
    ON rfm.customer_id = bf.customer_id
  LEFT JOIN customer_purchase_patterns pp
    ON rfm.customer_id = pp.customer_id
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_marts.mart_customer_rfm` rfm_mart
    ON rfm.customer_id = rfm_mart.customer_id
),

normalized_features AS (
  
  SELECT 
    *,
   
    CASE 
      WHEN days_since_last_purchase = 0 THEN 100
      ELSE CAST(
        100 - (
          (days_since_last_purchase - MIN(days_since_last_purchase) OVER ()) /
          NULLIF(MAX(days_since_last_purchase) OVER () - MIN(days_since_last_purchase) OVER (), 0) * 100
        ) AS INT64
      )
    END AS recency_score,
    
    CAST(
      (total_orders - MIN(total_orders) OVER ()) /
      NULLIF(MAX(total_orders) OVER () - MIN(total_orders) OVER (), 0) * 100 
      AS INT64
    ) AS frequency_score,
    
    CAST(
      (total_spent - MIN(total_spent) OVER ()) /
      NULLIF(MAX(total_spent) OVER () - MIN(total_spent) OVER (), 0) * 100 
      AS INT64
    ) AS monetary_score,
    
    CAST(avg_review_score * 20 AS INT64) AS review_score_normalized,
    
    CAST(
      (num_categories_purchased - MIN(num_categories_purchased) OVER ()) /
      NULLIF(MAX(num_categories_purchased) OVER () - MIN(num_categories_purchased) OVER (), 0) * 100 
      AS INT64
    ) AS category_diversity_score
  FROM customer_features_combined
),

churn_target AS (

  SELECT 
    *,
    
    CASE 
      WHEN days_since_last_purchase > 180 THEN TRUE
      WHEN avg_days_between_purchases > 0 
           AND days_since_last_purchase > (2 * avg_days_between_purchases) THEN TRUE
      ELSE FALSE
    END AS is_churned,
   
    CASE 
      WHEN avg_days_between_purchases > 0 
      THEN GREATEST(180, 2 * avg_days_between_purchases)
      ELSE 180
    END AS churn_threshold_days
  FROM normalized_features
),

churn_risk_scoring AS (
  
  SELECT 
    *,
    
    CAST(
      (
        -- Recency contributes 40%
        (100 - recency_score) * 0.40 +
        -- Low frequency contributes 25%
        (100 - frequency_score) * 0.25 +
        -- Low monetary contributes 15%
        (100 - monetary_score) * 0.15 +
        -- Delivery issues contribute 10%
        (pct_delayed_orders * 100) * 0.10 +
        -- Low review score contributes 10%
        (100 - review_score_normalized) * 0.10
      ) AS INT64
    ) AS churn_risk_score,
    
    CASE 
      WHEN is_churned THEN 'Churned'
      WHEN (
        (100 - recency_score) * 0.40 +
        (100 - frequency_score) * 0.25 +
        (100 - monetary_score) * 0.15 +
        (pct_delayed_orders * 100) * 0.10 +
        (100 - review_score_normalized) * 0.10
      ) >= 70 THEN 'High Risk'
      WHEN (
        (100 - recency_score) * 0.40 +
        (100 - frequency_score) * 0.25 +
        (100 - monetary_score) * 0.15 +
        (pct_delayed_orders * 100) * 0.10 +
        (100 - review_score_normalized) * 0.10
      ) >= 40 THEN 'Medium Risk'
      ELSE 'Low Risk'
    END AS churn_risk_segment,
    -- Risk percentile
    PERCENT_RANK() OVER (ORDER BY 
      (
        (100 - recency_score) * 0.40 +
        (100 - frequency_score) * 0.25 +
        (100 - monetary_score) * 0.15 +
        (pct_delayed_orders * 100) * 0.10 +
        (100 - review_score_normalized) * 0.10
      )
    ) AS churn_risk_percentile
  FROM churn_target
)


SELECT 
  customer_id,
  customer_state,
  customer_region,

  days_since_last_purchase,
  recency_score,
  first_purchase_date,
  last_purchase_date,
  customer_lifespan_days,
  
  total_orders,
  frequency_score,
  avg_days_between_purchases,
  stddev_days_between_purchases,
  
  total_spent,
  avg_order_value,
  monetary_score,
 
  preferred_payment_method,
  avg_installments,
  pct_delayed_orders,
  avg_delivery_delay,
  avg_review_score,
  review_score_normalized,
  total_freight_paid,
  avg_freight_per_order,
  num_categories_purchased,
  category_diversity_score,
  preferred_category,

  rfm_segment,
  rfm_score,
 
  is_churned,
  churn_threshold_days,
  churn_risk_score,
  churn_risk_segment,
  ROUND(churn_risk_percentile * 100, 2) AS churn_risk_percentile_pct
FROM churn_risk_scoring
ORDER BY churn_risk_score DESC;

/*
--VALIDATION QUERIES
-- 1. Check overall churn rate
SELECT 
  COUNT(*) AS total_customers,
  COUNTIF(is_churned) AS churned_customers,
  ROUND(COUNTIF(is_churned) / COUNT(*) * 100, 2) AS churn_rate_pct
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`;

-- 2. Churn rate by risk segment
SELECT 
  churn_risk_segment,
  COUNT(*) AS customers,
  COUNTIF(is_churned) AS churned,
  ROUND(COUNTIF(is_churned) / COUNT(*) * 100, 2) AS churn_rate_pct,
  ROUND(AVG(churn_risk_score), 1) AS avg_risk_score,
  ROUND(AVG(days_since_last_purchase), 1) AS avg_days_since_purchase
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`
GROUP BY churn_risk_segment
ORDER BY avg_risk_score DESC;

-- 3. Churn rate by RFM segment
SELECT 
  rfm_segment,
  COUNT(*) AS customers,
  COUNTIF(is_churned) AS churned,
  ROUND(COUNTIF(is_churned) / COUNT(*) * 100, 2) AS churn_rate_pct,
  ROUND(AVG(churn_risk_score), 1) AS avg_risk_score,
  ROUND(AVG(total_spent), 2) AS avg_ltv
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`
GROUP BY rfm_segment
ORDER BY churn_rate_pct DESC;

-- 4. Churn rate by state
SELECT 
  customer_state,
  COUNT(*) AS customers,
  COUNTIF(is_churned) AS churned,
  ROUND(COUNTIF(is_churned) / COUNT(*) * 100, 2) AS churn_rate_pct,
  ROUND(AVG(churn_risk_score), 1) AS avg_risk_score
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`
GROUP BY customer_state
ORDER BY customers DESC
LIMIT 10;

-- 5. Churn rate by payment method
SELECT 
  preferred_payment_method,
  COUNT(*) AS customers,
  COUNTIF(is_churned) AS churned,
  ROUND(COUNTIF(is_churned) / COUNT(*) * 100, 2) AS churn_rate_pct,
  ROUND(AVG(churn_risk_score), 1) AS avg_risk_score
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`
GROUP BY preferred_payment_method
ORDER BY customers DESC;

-- 6. Top 100 at-risk customers (not yet churned)
SELECT 
  customer_id,
  customer_state,
  churn_risk_score,
  churn_risk_percentile_pct,
  days_since_last_purchase,
  total_orders,
  ROUND(total_spent, 2) AS total_spent,
  rfm_segment,
  avg_review_score,
  pct_delayed_orders
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`
WHERE is_churned = FALSE
ORDER BY churn_risk_score DESC
LIMIT 100;

-- 7. Feature importance proxy (correlation with churn)
SELECT 
  'High Recency' AS feature,
  ROUND(AVG(CASE WHEN is_churned THEN recency_score ELSE 0 END), 2) AS avg_when_churned,
  ROUND(AVG(CASE WHEN NOT is_churned THEN recency_score ELSE 0 END), 2) AS avg_when_active
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`
UNION ALL
SELECT 
  'Low Frequency' AS feature,
  ROUND(AVG(CASE WHEN is_churned THEN frequency_score ELSE 0 END), 2),
  ROUND(AVG(CASE WHEN NOT is_churned THEN frequency_score ELSE 0 END), 2)
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`
UNION ALL
SELECT 
  'Low Monetary' AS feature,
  ROUND(AVG(CASE WHEN is_churned THEN monetary_score ELSE 0 END), 2),
  ROUND(AVG(CASE WHEN NOT is_churned THEN monetary_score ELSE 0 END), 2)
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`
UNION ALL
SELECT 
  'Delayed Orders %' AS feature,
  ROUND(AVG(CASE WHEN is_churned THEN pct_delayed_orders * 100 ELSE 0 END), 2),
  ROUND(AVG(CASE WHEN NOT is_churned THEN pct_delayed_orders * 100 ELSE 0 END), 2)
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`
UNION ALL
SELECT 
  'Low Review Score' AS feature,
  ROUND(AVG(CASE WHEN is_churned THEN avg_review_score ELSE 0 END), 2),
  ROUND(AVG(CASE WHEN NOT is_churned THEN avg_review_score ELSE 0 END), 2)
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`;

-- 8. Distribution of risk scores
SELECT 
  FLOOR(churn_risk_score / 10) * 10 AS risk_score_bin,
  COUNT(*) AS customers,
  COUNTIF(is_churned) AS churned,
  ROUND(COUNTIF(is_churned) / COUNT(*) * 100, 2) AS churn_rate_pct
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`
GROUP BY risk_score_bin
ORDER BY risk_score_bin;

-- 9. Check for nulls in critical columns
SELECT 
  COUNTIF(customer_id IS NULL) AS null_customer_id,
  COUNTIF(days_since_last_purchase IS NULL) AS null_recency,
  COUNTIF(total_orders IS NULL) AS null_frequency,
  COUNTIF(total_spent IS NULL) AS null_monetary,
  COUNTIF(churn_risk_score IS NULL) AS null_risk_score
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`;

-- 10. Top 10% at-risk customers for priority outreach
SELECT 
  COUNT(*) AS top_10_pct_customers,
  ROUND(AVG(total_spent), 2) AS avg_ltv_at_risk,
  ROUND(SUM(total_spent), 2) AS total_ltv_at_risk,
  ROUND(AVG(days_since_last_purchase), 1) AS avg_days_inactive
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_churn_prediction`
WHERE churn_risk_percentile_pct >= 90 
  AND is_churned = FALSE;

=============================================================================
*/