-- =========================================================
-- MART: CUSTOMER LTV
-- =========================================================
-- Description: Customer Lifetime Value analysis (customer and state level)
-- Based on: Analysis #1 (LTV by State) + Analysis #15 (CLV Histórico)
-- Source: mart_customer_base, stg_orders, stg_payments, stg_customers
-- Destination: olist_marts.mart_customer_ltv
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_customer_ltv` AS

WITH customer_orders AS (
  SELECT
    o.customer_id,
    MIN(o.order_purchase_timestamp) AS first_purchase_date,
    MAX(o.order_purchase_timestamp) AS last_purchase_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    DATE_DIFF(MAX(o.order_purchase_timestamp), MIN(o.order_purchase_timestamp), DAY) AS customer_lifespan_days
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  WHERE o.is_delivered OR o.is_completed
  GROUP BY o.customer_id
),

customer_revenue AS (
  SELECT
    o.customer_id,
    SUM(p.payment_value) AS total_revenue
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  WHERE o.is_delivered OR o.is_completed
  GROUP BY o.customer_id
),

customer_ltv_metrics AS (

  SELECT
    c.customer_id,
    c.customer_state,
    c.customer_city,
    c.customer_region,
    
    COALESCE(cr.total_revenue, 0) AS total_revenue,
    COALESCE(co.total_orders, 0) AS total_orders,
    
    co.first_purchase_date,
    co.last_purchase_date,
    COALESCE(co.customer_lifespan_days, 0) AS customer_lifespan_days,
    
    ROUND(COALESCE(cr.total_revenue, 0) / NULLIF(co.total_orders, 0), 2) AS avg_order_value,
    ROUND(COALESCE(co.total_orders, 0) / NULLIF(co.customer_lifespan_days / 30.0, 0), 2) AS orders_per_month,
    
    CASE
      WHEN COALESCE(cr.total_revenue, 0) >= PERCENTILE_CONT(cr.total_revenue, 0.90) OVER () THEN 'VIP'
      WHEN COALESCE(cr.total_revenue, 0) >= PERCENTILE_CONT(cr.total_revenue, 0.75) OVER () THEN 'High'
      WHEN COALESCE(cr.total_revenue, 0) >= PERCENTILE_CONT(cr.total_revenue, 0.50) OVER () THEN 'Medium'
      ELSE 'Low'
    END AS clv_segment,
    
    CASE WHEN co.total_orders > 1 THEN TRUE ELSE FALSE END AS is_repeat_customer
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_customers` c
  LEFT JOIN customer_orders co
    ON c.customer_id = co.customer_id
  LEFT JOIN customer_revenue cr
    ON c.customer_id = cr.customer_id
  WHERE co.customer_id IS NOT NULL
),

state_aggregations AS (

  SELECT
    customer_state,
    customer_region,
    
    COUNT(DISTINCT customer_id) AS total_customers_state,
    
    ROUND(AVG(total_revenue), 2) AS avg_ltv_state,
    ROUND(APPROX_QUANTILES(total_revenue, 100)[OFFSET(50)], 2) AS median_ltv_state,
    ROUND(SUM(total_revenue), 2) AS total_revenue_state,
    
    ROUND(AVG(avg_order_value), 2) AS avg_aov_state,
    
    ROUND(COUNTIF(is_repeat_customer) * 100.0 / COUNT(*), 2) AS repurchase_rate_state,
    
    ROUND(SUM(total_revenue) * 100.0 / SUM(SUM(total_revenue)) OVER (), 2) AS revenue_concentration_pct
    
  FROM customer_ltv_metrics
  GROUP BY customer_state, customer_region
),

final_with_state_metrics AS (

  SELECT
    clm.*,
    
    sa.total_customers_state,
    sa.avg_ltv_state,
    sa.median_ltv_state,
    sa.total_revenue_state,
    sa.avg_aov_state,
    sa.repurchase_rate_state,
    sa.revenue_concentration_pct,
    
    ROUND(sa.total_customers_state * 1.0 / NULLIF(sa.total_revenue_state, 0) * 100000, 2) AS customer_density_proxy,
    
    DENSE_RANK() OVER (ORDER BY sa.avg_ltv_state DESC) AS state_ltv_rank
    
  FROM customer_ltv_metrics clm
  LEFT JOIN state_aggregations sa
    ON clm.customer_state = sa.customer_state
)

SELECT *
FROM final_with_state_metrics
ORDER BY total_revenue DESC, customer_state;


/*
-- VALIDATION QUERIES
-- 1. Customer-level LTV summary (Analysis #15)
SELECT 
  COUNT(*) AS total_customers,
  ROUND(AVG(total_revenue), 2) AS avg_clv,
  ROUND(APPROX_QUANTILES(total_revenue, 100)[OFFSET(50)], 2) AS median_clv,
  ROUND(MAX(total_revenue), 2) AS max_clv,
  ROUND(MIN(total_revenue), 2) AS min_clv
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_ltv`;

-- 2. CLV distribution by segment (Analysis #15)
SELECT 
  clv_segment,
  COUNT(*) AS customers,
  ROUND(AVG(total_revenue), 2) AS avg_revenue,
  ROUND(SUM(total_revenue), 2) AS total_revenue,
  ROUND(SUM(total_revenue) * 100.0 / SUM(SUM(total_revenue)) OVER (), 2) AS revenue_pct
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_ltv`
GROUP BY clv_segment
ORDER BY 
  CASE clv_segment
    WHEN 'VIP' THEN 1
    WHEN 'High' THEN 2
    WHEN 'Medium' THEN 3
    WHEN 'Low' THEN 4
  END;

-- 3. Top 1%, 5%, 10% customers (Analysis #15)
WITH customer_revenue_rank AS (
  SELECT 
    customer_id,
    total_revenue,
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    COUNT(*) OVER () AS total_customers
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_ltv`
)
SELECT
  'Top 1%' AS segment,
  COUNT(*) AS customers,
  ROUND(SUM(total_revenue), 2) AS total_revenue,
  ROUND(SUM(total_revenue) * 100.0 / (SELECT SUM(total_revenue) FROM customer_revenue_rank), 2) AS revenue_pct
FROM customer_revenue_rank
WHERE revenue_rank <= CAST(total_customers * 0.01 AS INT64)
UNION ALL
SELECT
  'Top 5%',
  COUNT(*),
  ROUND(SUM(total_revenue), 2),
  ROUND(SUM(total_revenue) * 100.0 / (SELECT SUM(total_revenue) FROM customer_revenue_rank), 2)
FROM customer_revenue_rank
WHERE revenue_rank <= CAST(total_customers * 0.05 AS INT64)
UNION ALL
SELECT
  'Top 10%',
  COUNT(*),
  ROUND(SUM(total_revenue), 2),
  ROUND(SUM(total_revenue) * 100.0 / (SELECT SUM(total_revenue) FROM customer_revenue_rank), 2)
FROM customer_revenue_rank
WHERE revenue_rank <= CAST(total_customers * 0.10 AS INT64);

-- 4. State-level LTV metrics (Analysis #1)
SELECT DISTINCT
  customer_state,
  customer_region,
  total_customers_state,
  avg_ltv_state,
  median_ltv_state,
  total_revenue_state,
  avg_aov_state,
  repurchase_rate_state,
  revenue_concentration_pct,
  state_ltv_rank
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_ltv`
ORDER BY state_ltv_rank;

-- 5. Revenue concentration (Pareto analysis)
WITH state_revenue AS (
  SELECT DISTINCT
    customer_state,
    total_revenue_state,
    revenue_concentration_pct,
    SUM(revenue_concentration_pct) OVER (ORDER BY total_revenue_state DESC) AS cumulative_pct
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_ltv`
)
SELECT 
  customer_state,
  ROUND(total_revenue_state, 2) AS revenue,
  ROUND(revenue_concentration_pct, 2) AS pct_of_total,
  ROUND(cumulative_pct, 2) AS cumulative_pct
FROM state_revenue
ORDER BY total_revenue_state DESC;

-- 6. Repurchase rate by state (Analysis #1)
SELECT DISTINCT
  customer_state,
  total_customers_state,
  repurchase_rate_state,
  avg_ltv_state
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_ltv`
ORDER BY repurchase_rate_state DESC;
*/