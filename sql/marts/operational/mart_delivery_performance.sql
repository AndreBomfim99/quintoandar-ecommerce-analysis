-- =========================================================
-- MART: DELIVERY PERFORMANCE
-- =========================================================
-- Description: Delivery performance analysis with SLA compliance and delay metrics
-- Based on: Analysis #6 (Delivery Analysis)
-- Source: stg_orders, stg_customers, stg_sellers, stg_order_items, stg_reviews
-- Destination: olist_marts.mart_delivery_performance
-- Granularity: 1 row per order (with delivery info)
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance` AS

WITH orders_with_delivery AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.order_status,
    o.is_delivered
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  WHERE o.order_delivered_customer_date IS NOT NULL
    AND o.order_estimated_delivery_date IS NOT NULL
),

delivery_times AS (
  
  SELECT
    owd.*,
    
    DATE_DIFF(
      DATE(owd.order_delivered_customer_date),
      DATE(owd.order_purchase_timestamp),
      DAY
    ) AS delivery_time_actual,
    
    DATE_DIFF(
      DATE(owd.order_estimated_delivery_date),
      DATE(owd.order_purchase_timestamp),
      DAY
    ) AS delivery_time_estimated,
    
    DATE_DIFF(
      DATE(owd.order_delivered_carrier_date),
      DATE(owd.order_purchase_timestamp),
      DAY
    ) AS carrier_time,
    
    DATE_DIFF(
      DATE(owd.order_delivered_customer_date),
      DATE(owd.order_delivered_carrier_date),
      DAY
    ) AS transit_time
    
  FROM orders_with_delivery owd
),

delivery_delays AS (
  
  SELECT
    dt.*,
    
    dt.delivery_time_actual - dt.delivery_time_estimated AS delivery_delay,
    
    dt.delivery_time_actual > dt.delivery_time_estimated AS is_delayed,
    
    CASE
      WHEN dt.delivery_time_actual <= dt.delivery_time_estimated THEN 'On Time'
      WHEN dt.delivery_time_actual - dt.delivery_time_estimated BETWEEN 1 AND 3 THEN '1-3 days late'
      WHEN dt.delivery_time_actual - dt.delivery_time_estimated BETWEEN 4 AND 7 THEN '4-7 days late'
      WHEN dt.delivery_time_actual - dt.delivery_time_estimated BETWEEN 8 AND 15 THEN '8-15 days late'
      WHEN dt.delivery_time_actual - dt.delivery_time_estimated > 15 THEN '15+ days late'
      ELSE 'Unknown'
    END AS delay_severity
    
  FROM delivery_times dt
),

orders_with_geography AS (
  SELECT
    dd.*,
    c.customer_state,
    c.customer_city,
    c.customer_zip_code_prefix AS customer_zip
  FROM delivery_delays dd
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_customers` c
    ON dd.customer_id = c.customer_id
),

orders_with_sellers AS (
  SELECT
    owg.*,
    oi.seller_id,
    s.seller_state,
    s.seller_city,
    s.seller_zip_code_prefix AS seller_zip
  FROM orders_with_geography owg
  INNER JOIN (
    SELECT DISTINCT
      order_id,
      FIRST_VALUE(seller_id) OVER (PARTITION BY order_id ORDER BY price DESC) AS seller_id
    FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items`
  ) oi ON owg.order_id = oi.order_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_sellers` s
    ON oi.seller_id = s.seller_id
),

orders_with_freight AS (
  SELECT
    ows.*,
    COALESCE(oi.total_freight, 0) AS freight_value,
    ROUND(COALESCE(oi.total_freight, 0) / NULLIF(ows.delivery_time_actual, 0), 2) AS freight_per_day
  FROM orders_with_sellers ows
  LEFT JOIN (
    SELECT
      order_id,
      SUM(freight_value) AS total_freight
    FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items`
    GROUP BY order_id
  ) oi ON ows.order_id = oi.order_id
),

orders_with_reviews AS (
  SELECT
    owf.*,
    r.review_score
  FROM orders_with_freight owf
  LEFT JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_reviews` r
    ON owf.order_id = r.order_id
),

final_with_metrics AS (
  SELECT
    owr.*,
    
    ABS(CAST(owr.customer_zip AS INT64) - CAST(owr.seller_zip AS INT64)) AS distance_proxy,
    
    CONCAT(owr.seller_state, ' → ', owr.customer_state) AS delivery_route,
    
    owr.seller_state = owr.customer_state AS is_same_state_delivery
    
  FROM orders_with_reviews owr
)

SELECT
  order_id,
  customer_id,
  seller_id,
  order_purchase_timestamp,
  order_delivered_customer_date,
  order_estimated_delivery_date,
  customer_state,
  customer_city,
  seller_state,
  seller_city,
  delivery_route,
  is_same_state_delivery,
  delivery_time_actual,
  delivery_time_estimated,
  carrier_time,
  transit_time,
  delivery_delay,
  is_delayed,
  delay_severity,
  distance_proxy,
  freight_value,
  freight_per_day,
  review_score
FROM final_with_metrics
ORDER BY order_purchase_timestamp DESC;


/*
-- VALIDATION QUERIES
-- 1. Overall SLA compliance
SELECT
  COUNT(*) AS total_orders,
  COUNTIF(NOT is_delayed) AS on_time_orders,
  COUNTIF(is_delayed) AS delayed_orders,
  ROUND(COUNTIF(NOT is_delayed) * 100.0 / COUNT(*), 2) AS sla_compliance_rate,
  ROUND(AVG(delivery_time_actual), 1) AS avg_delivery_days_actual,
  ROUND(AVG(delivery_time_estimated), 1) AS avg_delivery_days_estimated,
  ROUND(AVG(CASE WHEN is_delayed THEN delivery_delay END), 1) AS avg_delay_when_late
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance`;

-- 2. Delay severity distribution
SELECT
  delay_severity,
  COUNT(*) AS num_orders,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_orders,
  ROUND(AVG(delivery_delay), 1) AS avg_delay_days,
  ROUND(AVG(review_score), 2) AS avg_review_score
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance`
GROUP BY delay_severity
ORDER BY 
  CASE delay_severity
    WHEN 'On Time' THEN 1
    WHEN '1-3 days late' THEN 2
    WHEN '4-7 days late' THEN 3
    WHEN '8-15 days late' THEN 4
    WHEN '15+ days late' THEN 5
  END;

-- 3. SLA compliance by destination state
SELECT
  customer_state,
  COUNT(*) AS total_orders,
  COUNTIF(NOT is_delayed) AS on_time_orders,
  ROUND(COUNTIF(NOT is_delayed) * 100.0 / COUNT(*), 2) AS sla_compliance_rate,
  ROUND(AVG(delivery_time_actual), 1) AS avg_delivery_days,
  ROUND(AVG(delivery_delay), 1) AS avg_delay_days
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance`
GROUP BY customer_state
ORDER BY sla_compliance_rate DESC;

-- 4. SLA compliance by route (top routes)
SELECT
  delivery_route,
  COUNT(*) AS total_orders,
  ROUND(COUNTIF(NOT is_delayed) * 100.0 / COUNT(*), 2) AS sla_compliance_rate,
  ROUND(AVG(delivery_time_actual), 1) AS avg_delivery_days,
  ROUND(AVG(delivery_delay), 1) AS avg_delay_days,
  ROUND(AVG(freight_value), 2) AS avg_freight
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance`
GROUP BY delivery_route
HAVING COUNT(*) >= 50  -- Only routes with significant volume
ORDER BY total_orders DESC
LIMIT 20;

-- 5. Same state vs cross-state delivery
SELECT
  is_same_state_delivery,
  COUNT(*) AS total_orders,
  ROUND(COUNTIF(NOT is_delayed) * 100.0 / COUNT(*), 2) AS sla_compliance_rate,
  ROUND(AVG(delivery_time_actual), 1) AS avg_delivery_days,
  ROUND(AVG(carrier_time), 1) AS avg_carrier_time,
  ROUND(AVG(transit_time), 1) AS avg_transit_time,
  ROUND(AVG(freight_value), 2) AS avg_freight
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance`
GROUP BY is_same_state_delivery;

-- 6. Correlation: Delay vs Review Score
SELECT
  delay_severity,
  COUNT(*) AS orders_with_reviews,
  ROUND(AVG(review_score), 2) AS avg_review_score,
  ROUND(AVG(delivery_delay), 1) AS avg_delay_days
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance`
WHERE review_score IS NOT NULL
GROUP BY delay_severity
ORDER BY 
  CASE delay_severity
    WHEN 'On Time' THEN 1
    WHEN '1-3 days late' THEN 2
    WHEN '4-7 days late' THEN 3
    WHEN '8-15 days late' THEN 4
    WHEN '15+ days late' THEN 5
  END;

-- 7. Freight analysis
SELECT
  delay_severity,
  COUNT(*) AS total_orders,
  ROUND(AVG(freight_value), 2) AS avg_freight,
  ROUND(AVG(freight_per_day), 2) AS avg_freight_per_day,
  ROUND(AVG(delivery_time_actual), 1) AS avg_delivery_days
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance`
GROUP BY delay_severity
ORDER BY 
  CASE delay_severity
    WHEN 'On Time' THEN 1
    WHEN '1-3 days late' THEN 2
    WHEN '4-7 days late' THEN 3
    WHEN '8-15 days late' THEN 4
    WHEN '15+ days late' THEN 5
  END;

-- 8. Carrier vs Transit time analysis
SELECT
  CASE
    WHEN carrier_time <= 2 THEN 'Fast pickup (0-2 days)'
    WHEN carrier_time BETWEEN 3 AND 5 THEN 'Normal pickup (3-5 days)'
    ELSE 'Slow pickup (6+ days)'
  END AS carrier_speed,
  COUNT(*) AS total_orders,
  ROUND(AVG(carrier_time), 1) AS avg_carrier_days,
  ROUND(AVG(transit_time), 1) AS avg_transit_days,
  ROUND(AVG(delivery_time_actual), 1) AS avg_total_delivery_days,
  ROUND(COUNTIF(NOT is_delayed) * 100.0 / COUNT(*), 2) AS sla_compliance_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance`
WHERE carrier_time IS NOT NULL
GROUP BY carrier_speed;

-- 9. Distance proxy impact (simplified)
SELECT
  CASE
    WHEN distance_proxy < 5000 THEN 'Near (<5k zip diff)'
    WHEN distance_proxy BETWEEN 5000 AND 20000 THEN 'Medium (5k-20k)'
    ELSE 'Far (20k+)'
  END AS distance_category,
  COUNT(*) AS total_orders,
  ROUND(AVG(delivery_time_actual), 1) AS avg_delivery_days,
  ROUND(COUNTIF(NOT is_delayed) * 100.0 / COUNT(*), 2) AS sla_compliance_rate,
  ROUND(AVG(freight_value), 2) AS avg_freight
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance`
GROUP BY distance_category;
*/