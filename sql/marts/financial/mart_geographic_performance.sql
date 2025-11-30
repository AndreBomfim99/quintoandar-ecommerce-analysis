-- =========================================================
-- MART: GEOGRAPHIC PERFORMANCE
-- =========================================================
-- Description: State-level performance analysis
-- Based on: Analysis #1 (LTV by State - Geographic part)
-- Source: mart_customer_ltv
-- Destination: olist_marts.mart_geographic_performance
-- Granularity: 1 row per state
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_geographic_performance` AS

WITH state_metrics AS (
  
  SELECT
    customer_state,
    customer_region,
    
  
    COUNT(DISTINCT customer_id) AS total_customers,
    COUNTIF(is_repeat_customer) AS repeat_customers,
    
    
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    ROUND(AVG(total_revenue), 2) AS avg_ltv,
    ROUND(APPROX_QUANTILES(total_revenue, 100)[OFFSET(50)], 2) AS median_ltv,
    ROUND(MAX(total_revenue), 2) AS max_ltv,
    ROUND(MIN(total_revenue), 2) AS min_ltv,
    
    
    SUM(total_orders) AS total_orders,
    ROUND(AVG(total_orders), 2) AS avg_orders_per_customer,
    
    ROUND(AVG(avg_order_value), 2) AS avg_aov,
    
    ROUND(COUNTIF(is_repeat_customer) * 100.0 / COUNT(*), 2) AS repurchase_rate,
    
    ROUND(AVG(customer_lifespan_days), 2) AS avg_customer_lifespan_days,
    
    ROUND(AVG(orders_per_month), 2) AS avg_orders_per_month
    
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_ltv`
  GROUP BY customer_state, customer_region
),

state_with_concentration AS (
  
  SELECT
    sm.*,
    
    
    ROUND(sm.total_revenue * 100.0 / SUM(sm.total_revenue) OVER (), 2) AS revenue_concentration_pct,
    
    ROUND(SUM(sm.total_revenue) OVER (ORDER BY sm.total_revenue DESC) * 100.0 / SUM(sm.total_revenue) OVER (), 2) AS cumulative_revenue_pct,
    
    ROUND(sm.total_customers * 100.0 / SUM(sm.total_customers) OVER (), 2) AS customer_concentration_pct,
    
    DENSE_RANK() OVER (ORDER BY sm.avg_ltv DESC) AS ltv_rank,
    DENSE_RANK() OVER (ORDER BY sm.total_revenue DESC) AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY sm.total_customers DESC) AS customer_rank,
    DENSE_RANK() OVER (ORDER BY sm.repurchase_rate DESC) AS repurchase_rank
    
  FROM state_metrics sm
),

state_with_potential AS (

  SELECT
    swc.*,
    
    ROUND(swc.total_customers / NULLIF(swc.total_revenue / 1000000.0, 0), 2) AS customers_per_million_revenue,
    
    ROUND(swc.total_revenue / NULLIF(swc.total_customers, 0), 2) AS revenue_per_customer,
    
    CASE
      WHEN swc.revenue_concentration_pct >= 10 THEN 'High Penetration'
      WHEN swc.revenue_concentration_pct >= 5 THEN 'Medium Penetration'
      WHEN swc.revenue_concentration_pct >= 2 THEN 'Low Penetration'
      ELSE 'Very Low Penetration'
    END AS market_penetration_category,
    
    CASE
      WHEN swc.repurchase_rate >= 15 AND swc.avg_ltv >= (SELECT AVG(avg_ltv) FROM state_with_concentration) THEN 'High Potential'
      WHEN swc.repurchase_rate >= 10 OR swc.avg_ltv >= (SELECT AVG(avg_ltv) FROM state_with_concentration) THEN 'Medium Potential'
      ELSE 'Low Potential'
    END AS growth_potential
    
  FROM state_with_concentration swc
)

SELECT
  customer_state,
  customer_region,
  total_customers,
  repeat_customers,
  total_revenue,
  avg_ltv,
  median_ltv,
  max_ltv,
  min_ltv,
  total_orders,
  avg_orders_per_customer,
  avg_aov,
  repurchase_rate,
  avg_customer_lifespan_days,
  avg_orders_per_month,
  revenue_concentration_pct,
  cumulative_revenue_pct,
  customer_concentration_pct,
  ltv_rank,
  revenue_rank,
  customer_rank,
  repurchase_rank,
  customers_per_million_revenue,
  revenue_per_customer,
  market_penetration_category,
  growth_potential
FROM state_with_potential
ORDER BY total_revenue DESC;


/*
-- VALIDATION QUERIES
-- 1. Top 10 states by revenue
SELECT 
  customer_state,
  customer_region,
  total_customers,
  ROUND(total_revenue, 2) AS revenue,
  revenue_concentration_pct,
  avg_ltv,
  repurchase_rate,
  growth_potential
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_geographic_performance`
ORDER BY total_revenue DESC
LIMIT 10;

-- 2. Revenue concentration (80/20 rule)
SELECT 
  COUNT(*) AS num_states,
  SUM(total_revenue) AS total_revenue,
  SUM(revenue_concentration_pct) AS pct_of_total_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_geographic_performance`
WHERE cumulative_revenue_pct <= 80;

-- 3. Performance by region
SELECT 
  customer_region,
  COUNT(*) AS num_states,
  SUM(total_customers) AS total_customers,
  ROUND(SUM(total_revenue), 2) AS total_revenue,
  ROUND(AVG(avg_ltv), 2) AS avg_ltv,
  ROUND(AVG(repurchase_rate), 2) AS avg_repurchase_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_geographic_performance`
GROUP BY customer_region
ORDER BY total_revenue DESC;

-- 4. States by growth potential
SELECT 
  growth_potential,
  COUNT(*) AS num_states,
  ROUND(AVG(avg_ltv), 2) AS avg_ltv,
  ROUND(AVG(repurchase_rate), 2) AS avg_repurchase_rate,
  ROUND(SUM(total_revenue), 2) AS total_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_geographic_performance`
GROUP BY growth_potential
ORDER BY 
  CASE growth_potential
    WHEN 'High Potential' THEN 1
    WHEN 'Medium Potential' THEN 2
    WHEN 'Low Potential' THEN 3
  END;

-- 5. Market penetration analysis
SELECT 
  market_penetration_category,
  COUNT(*) AS num_states,
  SUM(total_customers) AS total_customers,
  ROUND(SUM(total_revenue), 2) AS total_revenue,
  ROUND(SUM(revenue_concentration_pct), 2) AS total_revenue_pct
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_geographic_performance`
GROUP BY market_penetration_category
ORDER BY 
  CASE market_penetration_category
    WHEN 'High Penetration' THEN 1
    WHEN 'Medium Penetration' THEN 2
    WHEN 'Low Penetration' THEN 3
    WHEN 'Very Low Penetration' THEN 4
  END;

-- 6. State ranking summary
SELECT 
  customer_state,
  customer_region,
  ltv_rank,
  revenue_rank,
  customer_rank,
  repurchase_rank,
  ROUND((ltv_rank + revenue_rank + customer_rank + repurchase_rank) / 4.0, 2) AS avg_rank
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_geographic_performance`
ORDER BY avg_rank;
*/