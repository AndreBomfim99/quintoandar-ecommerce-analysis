-- =========================================================
-- MART: CUSTOMER BASE
-- =========================================================
-- Description: Consolidated customer view with all key metrics
-- Sources: stg_customers, stg_orders, stg_payments, stg_reviews
-- Destination: olist_marts.mart_customer_base
-- Granularity: 1 row per customer
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_customer_base` AS

WITH customer_orders AS (
 
  SELECT
    o.customer_id,
    
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(CASE WHEN o.is_delivered THEN 1 ELSE 0 END) AS delivered_orders,
    SUM(CASE WHEN o.is_canceled THEN 1 ELSE 0 END) AS canceled_orders,
    
    MIN(o.order_purchase_timestamp) AS first_purchase_date,
    MAX(o.order_purchase_timestamp) AS last_purchase_date,
    
    DATE_DIFF(
      MAX(o.order_purchase_timestamp), 
      MIN(o.order_purchase_timestamp), 
      DAY
    ) AS customer_lifespan_days
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  GROUP BY o.customer_id
),

customer_payments AS (
 
  SELECT
    o.customer_id,
    
 
    SUM(p.payment_value) AS total_revenue,
    AVG(p.payment_value) AS avg_order_value,
    MAX(p.payment_value) AS max_order_value,
    MIN(p.payment_value) AS min_order_value,
    

    ARRAY_AGG(p.payment_type ORDER BY p.payment_value DESC LIMIT 1)[OFFSET(0)] AS preferred_payment_method,
    AVG(p.payment_installments) AS avg_installments
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  GROUP BY o.customer_id
),

customer_reviews AS (
  SELECT
    o.customer_id,
    
    COUNT(DISTINCT r.review_id) AS total_reviews,
    AVG(r.review_score) AS avg_review_score,
    SUM(CASE WHEN r.has_comment THEN 1 ELSE 0 END) AS reviews_with_comment,
    
    SUM(CASE WHEN r.review_score >= 4 THEN 1 ELSE 0 END) AS positive_reviews,
    SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) AS negative_reviews
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_reviews` r
    ON o.order_id = r.order_id
  GROUP BY o.customer_id
),

customer_delivery AS (
  
  SELECT
    o.customer_id,
    
    
    AVG(
      DATE_DIFF(
        o.order_delivered_customer_date,
        o.order_purchase_timestamp,
        DAY
      )
    ) AS avg_delivery_days,
    
    
    SUM(
      CASE 
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date 
        THEN 1 
        ELSE 0 
      END
    ) AS delayed_orders,
    
    AVG(
      CASE 
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date 
        THEN DATE_DIFF(
          o.order_delivered_customer_date,
          o.order_estimated_delivery_date,
          DAY
        )
        ELSE 0
      END
    ) AS avg_delay_days
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  WHERE o.order_delivered_customer_date IS NOT NULL
  GROUP BY o.customer_id
),

customer_items AS (
  
  SELECT
    o.customer_id,

    COUNT(oi.order_item_id) AS total_items_purchased,
    AVG(oi.price) AS avg_item_price,
    SUM(oi.freight_value) AS total_freight_paid,
    AVG(oi.freight_value) AS avg_freight_per_order,
    
    COUNT(DISTINCT oi.product_id) AS unique_products_purchased,
    COUNT(DISTINCT oi.seller_id) AS unique_sellers_purchased_from
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
    ON o.order_id = oi.order_id
  GROUP BY o.customer_id
),

final_base AS (
  
  SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    c.customer_region,
    
    co.total_orders,
    co.delivered_orders,
    co.canceled_orders,
    
    co.first_purchase_date,
    co.last_purchase_date,
    co.customer_lifespan_days,
    
    DATE_DIFF(CURRENT_DATE(), DATE(co.last_purchase_date), DAY) AS recency_days,
    
    COALESCE(cp.total_revenue, 0) AS total_revenue,
    COALESCE(cp.avg_order_value, 0) AS avg_order_value,
    COALESCE(cp.max_order_value, 0) AS max_order_value,
    COALESCE(cp.min_order_value, 0) AS min_order_value,
    
    cp.preferred_payment_method,
    COALESCE(cp.avg_installments, 0) AS avg_installments,
    
    COALESCE(cr.total_reviews, 0) AS total_reviews,
    COALESCE(cr.avg_review_score, 0) AS avg_review_score,
    COALESCE(cr.reviews_with_comment, 0) AS reviews_with_comment,
    COALESCE(cr.positive_reviews, 0) AS positive_reviews,
    COALESCE(cr.negative_reviews, 0) AS negative_reviews,
    
    COALESCE(cd.avg_delivery_days, 0) AS avg_delivery_days,
    COALESCE(cd.delayed_orders, 0) AS delayed_orders,
    COALESCE(cd.avg_delay_days, 0) AS avg_delay_days,
    
    COALESCE(ci.total_items_purchased, 0) AS total_items_purchased,
    COALESCE(ci.avg_item_price, 0) AS avg_item_price,
    COALESCE(ci.total_freight_paid, 0) AS total_freight_paid,
    COALESCE(ci.avg_freight_per_order, 0) AS avg_freight_per_order,
    COALESCE(ci.unique_products_purchased, 0) AS unique_products_purchased,
    COALESCE(ci.unique_sellers_purchased_from, 0) AS unique_sellers_purchased_from,
    
    ROUND(COALESCE(cp.total_revenue, 0) / NULLIF(co.total_orders, 0), 2) AS revenue_per_order,
    ROUND(COALESCE(ci.total_items_purchased, 0) / NULLIF(co.total_orders, 0), 2) AS items_per_order,
    
    co.total_orders > 1 AS is_repeat_customer,
    DATE_DIFF(CURRENT_DATE(), DATE(co.last_purchase_date), DAY) > 180 AS is_churned,
    COALESCE(cr.avg_review_score, 0) >= 4 AS is_satisfied_customer,
    COALESCE(cd.delayed_orders, 0) / NULLIF(co.delivered_orders, 0) > 0.3 AS has_delivery_issues
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_customers` c
  INNER JOIN customer_orders co
    ON c.customer_id = co.customer_id
  LEFT JOIN customer_payments cp
    ON c.customer_id = cp.customer_id
  LEFT JOIN customer_reviews cr
    ON c.customer_id = cr.customer_id
  LEFT JOIN customer_delivery cd
    ON c.customer_id = cd.customer_id
  LEFT JOIN customer_items ci
    ON c.customer_id = ci.customer_id
)

SELECT *
FROM final_base
ORDER BY total_revenue DESC;


-- Uncomment and run separately to validate the mart:
/*
-- VALIDATION QUERIES
-- 1. Check total customers
SELECT 
  COUNT(*) AS total_customers,
  COUNT(DISTINCT customer_id) AS unique_customers
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_base`;

-- 2. Distribution of customer types
SELECT 
  is_repeat_customer,
  COUNT(*) AS customer_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_base`
GROUP BY is_repeat_customer;

-- 3. Revenue concentration
SELECT 
  ROUND(SUM(CASE WHEN revenue_rank <= total_customers * 0.01 THEN total_revenue ELSE 0 END) / SUM(total_revenue) * 100, 2) AS top_1_pct_revenue,
  ROUND(SUM(CASE WHEN revenue_rank <= total_customers * 0.10 THEN total_revenue ELSE 0 END) / SUM(total_revenue) * 100, 2) AS top_10_pct_revenue
FROM (
  SELECT 
    total_revenue,
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    COUNT(*) OVER () AS total_customers
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_base`
);

-- 4. Customer metrics summary
SELECT 
  ROUND(AVG(total_orders), 2) AS avg_orders,
  ROUND(AVG(total_revenue), 2) AS avg_revenue,
  ROUND(AVG(avg_review_score), 2) AS avg_review,
  ROUND(AVG(recency_days), 2) AS avg_recency_days,
  ROUND(AVG(customer_lifespan_days), 2) AS avg_lifespan_days
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_base`;

-- 5. Customer status distribution
SELECT 
  COUNTIF(is_repeat_customer) AS repeat_customers,
  COUNTIF(is_churned) AS churned_customers,
  COUNTIF(is_satisfied_customer) AS satisfied_customers,
  COUNTIF(has_delivery_issues) AS customers_with_delivery_issues
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_base`;
*/