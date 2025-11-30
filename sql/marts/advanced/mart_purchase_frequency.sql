-- =========================================================
-- MART: PURCHASE FREQUENCY DISTRIBUTION
-- =========================================================
-- Description: Customer distribution by number of purchases
-- Sources: stg_orders, stg_payments, mart_customer_base
-- Destination: olist_marts.mart_purchase_frequency
-- Granularity: 1 row per customer with frequency segmentation
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_purchase_frequency` AS

WITH customer_order_counts AS (
  
  SELECT
    o.customer_id,
    COUNT(DISTINCT o.order_id) AS num_orders,
    SUM(p.payment_value) AS total_revenue,
    AVG(p.payment_value) AS avg_order_value,
    MIN(o.order_purchase_timestamp) AS first_purchase_date,
    MAX(o.order_purchase_timestamp) AS last_purchase_date
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  WHERE o.is_completed = TRUE
  GROUP BY o.customer_id
),

customer_frequency_bins AS (
  
  SELECT
    coc.customer_id,
    coc.num_orders,
    coc.total_revenue,
    coc.avg_order_value,
    coc.first_purchase_date,
    coc.last_purchase_date,
    
    
    CASE
      WHEN coc.num_orders = 1 THEN '1x'
      WHEN coc.num_orders = 2 THEN '2x' 
      WHEN coc.num_orders BETWEEN 3 AND 5 THEN '3-5x'
      WHEN coc.num_orders BETWEEN 6 AND 10 THEN '6-10x'
      ELSE '11+'
    END AS frequency_bin,
    
    DATE_DIFF(CURRENT_DATE(), DATE(coc.first_purchase_date), DAY) AS customer_tenure_days,
    
    CASE 
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(coc.first_purchase_date), DAY) > 0
      THEN coc.num_orders / (DATE_DIFF(CURRENT_DATE(), DATE(coc.first_purchase_date), DAY) / 30.0)
      ELSE coc.num_orders
    END AS monthly_purchase_rate

  FROM customer_order_counts coc
),

aggregate_metrics AS (
  SELECT
    cfb.frequency_bin,
    COUNT(*) AS customer_count,
    SUM(cfb.total_revenue) AS total_revenue,
    AVG(cfb.num_orders) AS avg_orders_per_customer,
    AVG(cfb.total_revenue) AS avg_revenue_per_customer,
    AVG(cfb.avg_order_value) AS avg_order_value,
    AVG(cfb.monthly_purchase_rate) AS avg_monthly_frequency,
    
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct_customers,
    SUM(cfb.total_revenue) * 100.0 / SUM(SUM(cfb.total_revenue)) OVER () AS pct_revenue,
    
    SUM(cfb.total_revenue) / COUNT(*) AS revenue_per_customer

  FROM customer_frequency_bins cfb
  GROUP BY cfb.frequency_bin
),

pareto_analysis AS (
  SELECT
    coc.customer_id,
    coc.num_orders,
    coc.total_revenue,
    
    SUM(coc.total_revenue) OVER (ORDER BY coc.total_revenue DESC) / SUM(coc.total_revenue) OVER () AS cumulative_revenue_pct,
    ROW_NUMBER() OVER (ORDER BY coc.total_revenue DESC) * 100.0 / COUNT(*) OVER () AS cumulative_customer_pct,
    
    CASE
      WHEN SUM(coc.total_revenue) OVER (ORDER BY coc.total_revenue DESC) / SUM(coc.total_revenue) OVER () <= 0.2 THEN 'Top_20%'
      WHEN SUM(coc.total_revenue) OVER (ORDER BY coc.total_revenue DESC) / SUM(coc.total_revenue) OVER () <= 0.5 THEN 'Next_30%'
      ELSE 'Bottom_50%'
    END AS pareto_segment

  FROM customer_order_counts coc
),

final_frequency_analysis AS (
  SELECT
    cfb.customer_id,
    cfb.num_orders,
    cfb.frequency_bin,
    cfb.total_revenue,
    cfb.avg_order_value,
    cfb.first_purchase_date,
    cfb.last_purchase_date,
    cfb.customer_tenure_days,
    cfb.monthly_purchase_rate,
    
    am.customer_count AS bin_customer_count,
    am.pct_customers AS bin_pct_customers,
    am.pct_revenue AS bin_pct_revenue,
    am.avg_orders_per_customer AS bin_avg_orders,
    am.avg_revenue_per_customer AS bin_avg_revenue,
    
    pa.cumulative_revenue_pct,
    pa.cumulative_customer_pct,
    pa.pareto_segment,
    
    CASE WHEN cfb.num_orders = 1 THEN 1 ELSE 0 END AS is_one_time_buyer,
    CASE WHEN cfb.num_orders >= 2 THEN 1 ELSE 0 END AS is_repeat_customer,
    CASE WHEN cfb.num_orders >= 6 THEN 1 ELSE 0 END AS is_frequent_customer,
    
    CASE
      WHEN cfb.total_revenue > 1000 THEN 'VIP'
      WHEN cfb.total_revenue > 500 THEN 'High_Value'
      WHEN cfb.total_revenue > 200 THEN 'Medium_Value'
      ELSE 'Standard'
    END AS value_segment

  FROM customer_frequency_bins cfb
  LEFT JOIN aggregate_metrics am ON cfb.frequency_bin = am.frequency_bin
  LEFT JOIN pareto_analysis pa ON cfb.customer_id = pa.customer_id
)

SELECT *
FROM final_frequency_analysis
ORDER BY num_orders DESC, total_revenue DESC;

/*
-- VALIDATION QUERIES
-- 1. Frequency distribution overview
SELECT
  frequency_bin,
  customer_count,
  ROUND(pct_customers, 2) AS pct_customers,
  ROUND(pct_revenue, 2) AS pct_revenue,
  ROUND(avg_orders_per_customer, 2) AS avg_orders,
  ROUND(avg_revenue_per_customer, 2) AS avg_revenue
FROM (
  SELECT
    frequency_bin,
    COUNT(*) AS customer_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct_customers,
    SUM(total_revenue) * 100.0 / SUM(SUM(total_revenue)) OVER () AS pct_revenue,
    AVG(num_orders) AS avg_orders_per_customer,
    AVG(total_revenue) AS avg_revenue_per_customer
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_purchase_frequency`
  GROUP BY frequency_bin
)
ORDER BY 
  CASE frequency_bin
    WHEN '1x' THEN 1
    WHEN '2x' THEN 2
    WHEN '3-5x' THEN 3
    WHEN '6-10x' THEN 4
    WHEN '11+' THEN 5
  END;

-- 2. One-time buyers analysis
SELECT
  COUNT(*) AS one_time_buyers,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `quintoandar-ecommerce-analysis.olist_marts.mart_purchase_frequency`), 2) AS pct_one_time_buyers,
  ROUND(AVG(total_revenue), 2) AS avg_one_time_revenue,
  ROUND(AVG(avg_order_value), 2) AS avg_one_time_order_value
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_purchase_frequency`
WHERE num_orders = 1;

-- 3. Pareto analysis (80/20 rule validation)
SELECT
  pareto_segment,
  COUNT(*) AS customer_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_customers,
  ROUND(SUM(total_revenue), 2) AS total_revenue,
  ROUND(SUM(total_revenue) * 100.0 / SUM(SUM(total_revenue)) OVER (), 2) AS pct_revenue,
  ROUND(AVG(total_revenue), 2) AS avg_revenue_per_customer
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_purchase_frequency`
GROUP BY pareto_segment
ORDER BY 
  CASE pareto_segment
    WHEN 'Top_20%' THEN 1
    WHEN 'Next_30%' THEN 2
    WHEN 'Bottom_50%' THEN 3
  END;

-- 4. Revenue concentration analysis
SELECT
  ROUND(SUM(CASE WHEN cumulative_customer_pct <= 20 THEN total_revenue ELSE 0 END) * 100.0 / SUM(total_revenue), 2) AS top_20_pct_revenue,
  ROUND(SUM(CASE WHEN cumulative_customer_pct <= 50 THEN total_revenue ELSE 0 END) * 100.0 / SUM(total_revenue), 2) AS top_50_pct_revenue,
  ROUND(SUM(CASE WHEN cumulative_customer_pct <= 80 THEN total_revenue ELSE 0 END) * 100.0 / SUM(total_revenue), 2) AS top_80_pct_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_purchase_frequency`;

-- 5. Frequency vs Value analysis
SELECT
  frequency_bin,
  value_segment,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_revenue,
  ROUND(AVG(monthly_purchase_rate), 3) AS avg_monthly_frequency
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_purchase_frequency`
GROUP BY frequency_bin, value_segment
ORDER BY frequency_bin, 
  CASE value_segment
    WHEN 'VIP' THEN 1
    WHEN 'High_Value' THEN 2
    WHEN 'Medium_Value' THEN 3
    WHEN 'Standard' THEN 4
  END;

-- 6. Customer tenure vs purchase frequency
SELECT
  frequency_bin,
  ROUND(AVG(customer_tenure_days), 1) AS avg_tenure_days,
  ROUND(AVG(num_orders), 2) AS avg_orders,
  ROUND(AVG(monthly_purchase_rate), 3) AS avg_monthly_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_purchase_frequency`
GROUP BY frequency_bin
ORDER BY 
  CASE frequency_bin
    WHEN '1x' THEN 1
    WHEN '2x' THEN 2
    WHEN '3-5x' THEN 3
    WHEN '6-10x' THEN 4
    WHEN '11+' THEN 5
  END;

-- 7. Data completeness check
SELECT
  COUNT(*) AS total_customers,
  COUNT(DISTINCT frequency_bin) AS unique_bins,
  ROUND(MIN(num_orders), 0) AS min_orders,
  ROUND(MAX(num_orders), 0) AS max_orders,
  ROUND(AVG(num_orders), 2) AS avg_orders_per_customer,
  ROUND(SUM(total_revenue), 2) AS total_revenue_analyzed
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_purchase_frequency`;
*/