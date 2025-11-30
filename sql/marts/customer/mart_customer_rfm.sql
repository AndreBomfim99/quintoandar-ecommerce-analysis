-- =========================================================
-- MART: CUSTOMER RFM
-- =========================================================
-- Description: RFM (Recency, Frequency, Monetary) customer segmentation
-- Based on: Analysis #4 (RFM Analysis)
-- Source: mart_customer_base, stg_orders, stg_payments
-- Destination: olist_marts.mart_customer_rfm
-- Granularity: 1 row per customer
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_customer_rfm` AS

WITH rfm_base AS (

  SELECT
    cb.customer_id,
    cb.customer_state,
    cb.customer_region,
    
    DATE_DIFF(CURRENT_DATE(), DATE(cb.last_purchase_date), DAY) AS recency,
    
    cb.total_orders AS frequency,
    
    cb.total_revenue AS monetary,
    
    cb.avg_order_value,
    cb.is_repeat_customer
    
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_base` cb
),

rfm_scores AS (

  SELECT
    rb.*,
    
    -- R Score: 5 = most recent (lowest recency days), 1 = least recent
    NTILE(5) OVER (ORDER BY rb.recency DESC) AS r_score,
    
    -- F Score: 5 = most frequent, 1 = least frequent
    NTILE(5) OVER (ORDER BY rb.frequency ASC) AS f_score,
    
    -- M Score: 5 = highest monetary value, 1 = lowest
    NTILE(5) OVER (ORDER BY rb.monetary ASC) AS m_score
    
  FROM rfm_base rb
),

rfm_combined AS (

  SELECT
    rs.*,
    
    CONCAT(
      CAST(rs.r_score AS STRING),
      CAST(rs.f_score AS STRING),
      CAST(rs.m_score AS STRING)
    ) AS rfm_score,
    
    rs.r_score + rs.f_score + rs.m_score AS rfm_total_score
    
  FROM rfm_scores rs
),

rfm_segments AS (

  SELECT
    rc.*,
    
    CASE
      -- Champions: Best customers (high R, F, M)
      WHEN rc.rfm_score IN ('555', '554', '544', '545', '454', '455', '445') THEN 'Champions'
      
      -- Loyal Customers: Frequent buyers (high F)
      WHEN rc.rfm_score IN ('543', '444', '435', '355', '354', '345', '344', '335') THEN 'Loyal Customers'
      
      -- Potential Loyalists: Recent customers with good value
      WHEN rc.rfm_score IN ('553', '551', '552', '541', '542', '533', '532', '531') THEN 'Potential Loyalists'
      
      -- New Customers: Recent but low frequency
      WHEN rc.rfm_score IN ('512', '511', '422', '421', '412', '411', '311') THEN 'New Customers'
      
      -- Promising: Recent with potential
      WHEN rc.rfm_score IN ('525', '524', '523', '522', '521', '515', '514', '513') THEN 'Promising'
      
      -- Need Attention: Above average but declining
      WHEN rc.rfm_score IN ('535', '534', '443', '434', '343', '334', '325', '324') THEN 'Need Attention'
      
      -- About to Sleep: Low recent activity
      WHEN rc.rfm_score IN ('331', '321', '312', '221', '213', '231', '241', '251') THEN 'About to Sleep'
      
      -- At Risk: Good customers who haven't purchased recently
      WHEN rc.rfm_score IN ('255', '254', '245', '244', '253', '252', '243', '242', '235', '234') THEN 'At Risk'
      
      -- Can't Lose Them: High value but at risk
      WHEN rc.rfm_score IN ('155', '154', '144', '214', '215', '115', '114', '113') THEN "Can't Lose Them"
      
      -- Hibernating: Low engagement
      WHEN rc.rfm_score IN ('332', '322', '231', '241', '335', '333', '323', '322') THEN 'Hibernating'
      
      -- Lost: Lowest engagement
      WHEN rc.rfm_score IN ('111', '112', '121', '131', '141', '151', '122', '132', '142', '152', '211', '212', '223') THEN 'Lost'
      
      ELSE 'Other'
    END AS rfm_segment
    
  FROM rfm_combined rc
)

SELECT
  customer_id,
  customer_state,
  customer_region,
  recency,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  rfm_score,
  rfm_total_score,
  rfm_segment,
  avg_order_value,
  is_repeat_customer
FROM rfm_segments
ORDER BY rfm_total_score DESC, monetary DESC;


/*
-- VALIDATION QUERIES
-- 1. Customer distribution by RFM segment
SELECT
  rfm_segment,
  COUNT(*) AS num_customers,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_customers,
  ROUND(AVG(recency), 1) AS avg_recency_days,
  ROUND(AVG(frequency), 1) AS avg_frequency,
  ROUND(AVG(monetary), 2) AS avg_monetary
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_rfm`
GROUP BY rfm_segment
ORDER BY num_customers DESC;

-- 2. Revenue by RFM segment
SELECT
  rfm_segment,
  COUNT(*) AS num_customers,
  ROUND(SUM(monetary), 2) AS total_revenue,
  ROUND(SUM(monetary) * 100.0 / SUM(SUM(monetary)) OVER (), 2) AS pct_revenue,
  ROUND(AVG(monetary), 2) AS avg_ltv,
  ROUND(AVG(avg_order_value), 2) AS avg_aov
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_rfm`
GROUP BY rfm_segment
ORDER BY total_revenue DESC;

-- 3. Repurchase rate by segment
SELECT
  rfm_segment,
  COUNT(*) AS total_customers,
  COUNTIF(is_repeat_customer) AS repeat_customers,
  ROUND(COUNTIF(is_repeat_customer) * 100.0 / COUNT(*), 2) AS repurchase_rate_pct
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_rfm`
GROUP BY rfm_segment
ORDER BY repurchase_rate_pct DESC;

-- 4. RFM score distribution
SELECT
  r_score,
  f_score,
  m_score,
  COUNT(*) AS num_customers,
  ROUND(AVG(monetary), 2) AS avg_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_rfm`
GROUP BY r_score, f_score, m_score
ORDER BY r_score DESC, f_score DESC, m_score DESC;

-- 5. Top segments by value
SELECT
  rfm_segment,
  COUNT(*) AS customers,
  ROUND(SUM(monetary), 2) AS total_revenue,
  ROUND(AVG(monetary), 2) AS avg_ltv,
  ROUND(AVG(frequency), 1) AS avg_orders,
  ROUND(AVG(recency), 0) AS avg_recency_days
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_rfm`
WHERE rfm_segment IN ('Champions', 'Loyal Customers', 'Potential Loyalists', "Can't Lose Them", 'At Risk')
GROUP BY rfm_segment
ORDER BY total_revenue DESC;

-- 6. Segment performance summary with recommendations
SELECT
  rfm_segment,
  COUNT(*) AS customers,
  ROUND(SUM(monetary), 2) AS revenue,
  ROUND(AVG(recency), 0) AS avg_recency,
  ROUND(AVG(frequency), 1) AS avg_frequency,
  ROUND(AVG(monetary), 2) AS avg_monetary,
  CASE
    WHEN rfm_segment = 'Champions' THEN 'Reward them. They are your best customers.'
    WHEN rfm_segment = 'Loyal Customers' THEN 'Upsell higher value products. Engage them.'
    WHEN rfm_segment = 'Potential Loyalists' THEN 'Offer membership/loyalty program.'
    WHEN rfm_segment = 'New Customers' THEN 'Provide on-boarding support, build relationship.'
    WHEN rfm_segment = 'Promising' THEN 'Create brand awareness, offer free trials.'
    WHEN rfm_segment = 'Need Attention' THEN 'Make limited time offers, recommend products.'
    WHEN rfm_segment = 'About to Sleep' THEN 'Share valuable resources, recommend popular products.'
    WHEN rfm_segment = 'At Risk' THEN 'Send personalized emails, offer renewals, provide helpful resources.'
    WHEN rfm_segment = "Can't Lose Them" THEN 'Win them back via renewals or newer products, reach out proactively.'
    WHEN rfm_segment = 'Hibernating' THEN 'Offer other products and special discounts. Recreate brand value.'
    WHEN rfm_segment = 'Lost' THEN 'Revive interest with reach out campaign, ignore otherwise.'
    ELSE 'Analyze further.'
  END AS recommended_action
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_rfm`
GROUP BY rfm_segment
ORDER BY 
  CASE rfm_segment
    WHEN 'Champions' THEN 1
    WHEN 'Loyal Customers' THEN 2
    WHEN 'Potential Loyalists' THEN 3
    WHEN 'New Customers' THEN 4
    WHEN 'Promising' THEN 5
    WHEN 'Need Attention' THEN 6
    WHEN 'About to Sleep' THEN 7
    WHEN 'At Risk' THEN 8
    WHEN "Can't Lose Them" THEN 9
    WHEN 'Hibernating' THEN 10
    WHEN 'Lost' THEN 11
    ELSE 12
  END;

-- 7. Geographic distribution by segment
SELECT
  customer_region,
  rfm_segment,
  COUNT(*) AS customers,
  ROUND(SUM(monetary), 2) AS revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_rfm`
GROUP BY customer_region, rfm_segment
ORDER BY customer_region, revenue DESC;
*/