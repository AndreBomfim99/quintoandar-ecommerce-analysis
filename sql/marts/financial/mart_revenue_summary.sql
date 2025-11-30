-- =========================================================
-- MART: REVENUE SUMMARY
-- =========================================================
-- Description: Revenue cohort analysis with temporal metrics and growth rates
-- Sources: stg_orders, stg_payments, mart_customer_base
-- Destination: olist_marts.mart_revenue_summary
-- Granularity: Multiple (daily, weekly, monthly aggregations)
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_revenue_summary` AS

WITH customer_cohorts AS (

  SELECT 
    customer_id,
    DATE_TRUNC(MIN(order_purchase_timestamp), MONTH) AS cohort_month
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders`
  WHERE is_completed = TRUE
  GROUP BY customer_id
),

order_revenue AS (
  
  SELECT 
    o.order_id,
    o.customer_id,
    c.cohort_month,
    DATE_TRUNC(o.order_purchase_timestamp, MONTH) AS order_month,
    DATE_TRUNC(o.order_purchase_timestamp, WEEK) AS order_week,
    DATE(o.order_purchase_timestamp) AS order_day,
    o.order_purchase_timestamp,
    p.payment_value,
    
    CASE 
      WHEN DATE_TRUNC(o.order_purchase_timestamp, MONTH) = c.cohort_month 
      THEN TRUE 
      ELSE FALSE 
    END AS is_first_order_month,
    
    DATE_DIFF(
      DATE(DATE_TRUNC(o.order_purchase_timestamp, MONTH)),
      DATE(c.cohort_month),
      MONTH
    ) AS months_since_cohort
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  INNER JOIN customer_cohorts c
    ON o.customer_id = c.customer_id
  WHERE o.is_completed = TRUE
),

daily_aggregation AS (
  SELECT
    order_day AS revenue_date,  
    'daily' AS time_period,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(payment_value) AS total_revenue,
    SUM(CASE WHEN is_first_order_month THEN payment_value ELSE 0 END) AS new_customer_revenue,
    SUM(CASE WHEN NOT is_first_order_month THEN payment_value ELSE 0 END) AS repeat_customer_revenue,
    COUNT(DISTINCT CASE WHEN is_first_order_month THEN customer_id END) AS new_customers,
    COUNT(DISTINCT CASE WHEN NOT is_first_order_month THEN customer_id END) AS repeat_customers,
    AVG(payment_value) AS avg_order_value
  FROM order_revenue
  GROUP BY order_day
),

weekly_aggregation AS (
  
  SELECT
    DATE(order_week) AS revenue_date,  
    'weekly' AS time_period,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(payment_value) AS total_revenue,
    SUM(CASE WHEN is_first_order_month THEN payment_value ELSE 0 END) AS new_customer_revenue,
    SUM(CASE WHEN NOT is_first_order_month THEN payment_value ELSE 0 END) AS repeat_customer_revenue,
    COUNT(DISTINCT CASE WHEN is_first_order_month THEN customer_id END) AS new_customers,
    COUNT(DISTINCT CASE WHEN NOT is_first_order_month THEN customer_id END) AS repeat_customers,
    AVG(payment_value) AS avg_order_value
  FROM order_revenue
  GROUP BY order_week
),

monthly_aggregation AS (
  
  SELECT
    DATE(order_month) AS revenue_date,  
    'monthly' AS time_period,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(payment_value) AS total_revenue,
    SUM(CASE WHEN is_first_order_month THEN payment_value ELSE 0 END) AS new_customer_revenue,
    SUM(CASE WHEN NOT is_first_order_month THEN payment_value ELSE 0 END) AS repeat_customer_revenue,
    COUNT(DISTINCT CASE WHEN is_first_order_month THEN customer_id END) AS new_customers,
    COUNT(DISTINCT CASE WHEN NOT is_first_order_month THEN customer_id END) AS repeat_customers,
    AVG(payment_value) AS avg_order_value
  FROM order_revenue
  GROUP BY order_month
),

combined_time_periods AS (
  
  SELECT * FROM daily_aggregation
  UNION ALL
  SELECT * FROM weekly_aggregation
  UNION ALL
  SELECT * FROM monthly_aggregation
),

revenue_with_growth AS (
  
  SELECT
    revenue_date,
    time_period,
    total_orders,
    total_customers,
    total_revenue,
    new_customer_revenue,
    repeat_customer_revenue,
    new_customers,
    repeat_customers,
    avg_order_value,
    

    SUM(total_revenue) OVER (
      PARTITION BY time_period 
      ORDER BY revenue_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue,
    

    CASE 
      WHEN time_period = 'monthly' THEN
        LAG(total_revenue, 1) OVER (
          PARTITION BY time_period 
          ORDER BY revenue_date
        )
    END AS previous_period_revenue,
    
    CASE 
      WHEN time_period = 'monthly' THEN
        SAFE_DIVIDE(
          total_revenue - LAG(total_revenue, 1) OVER (
            PARTITION BY time_period 
            ORDER BY revenue_date
          ),
          LAG(total_revenue, 1) OVER (
            PARTITION BY time_period 
            ORDER BY revenue_date
          )
        ) * 100
    END AS revenue_growth_mom_pct,
    
    CASE 
      WHEN time_period = 'monthly' THEN
        LAG(total_revenue, 12) OVER (
          PARTITION BY time_period 
          ORDER BY revenue_date
        )
    END AS previous_year_revenue,
    
    CASE 
      WHEN time_period = 'monthly' THEN
        SAFE_DIVIDE(
          total_revenue - LAG(total_revenue, 12) OVER (
            PARTITION BY time_period 
            ORDER BY revenue_date
          ),
          LAG(total_revenue, 12) OVER (
            PARTITION BY time_period 
            ORDER BY revenue_date
          )
        ) * 100
    END AS revenue_growth_yoy_pct
  FROM combined_time_periods
),

final_revenue_summary AS (
  
  SELECT
    revenue_date,
    time_period,
    total_orders,
    total_customers,
    new_customers,
    repeat_customers,
    total_revenue,
    new_customer_revenue,
    repeat_customer_revenue,
    avg_order_value,
    cumulative_revenue,
    previous_period_revenue,
    revenue_growth_mom_pct,
    previous_year_revenue,
    revenue_growth_yoy_pct,
    
    SAFE_DIVIDE(repeat_customers, total_customers) * 100 AS repeat_customer_rate,
    SAFE_DIVIDE(repeat_customer_revenue, total_revenue) * 100 AS repeat_revenue_rate,
    
    SAFE_DIVIDE(total_revenue, total_customers) AS revenue_per_customer,
    SAFE_DIVIDE(new_customer_revenue, new_customers) AS revenue_per_new_customer,
    SAFE_DIVIDE(repeat_customer_revenue, repeat_customers) AS revenue_per_repeat_customer,
    
    total_revenue > 10000 AS is_high_revenue_day,
    revenue_growth_mom_pct > 0 AS is_positive_growth,
    SAFE_DIVIDE(repeat_customer_revenue, total_revenue) > 0.3 AS has_high_repeat_rate
    
  FROM revenue_with_growth
)

SELECT *
FROM final_revenue_summary
ORDER BY time_period, revenue_date DESC;


-- Uncomment and run separately to validate the mart:
/*
-- VALIDATION QUERIES
-- 1. Check data distribution across time periods
SELECT 
  time_period,
  COUNT(*) AS period_count,
  MIN(revenue_date) AS first_date,
  MAX(revenue_date) AS last_date,
  ROUND(AVG(total_revenue), 2) AS avg_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_revenue_summary`
GROUP BY time_period
ORDER BY time_period;

-- 2. Verify revenue totals match source data
SELECT 
  ROUND(SUM(total_revenue), 2) AS mart_total_revenue,
  (SELECT ROUND(SUM(payment_value), 2)
   FROM `quintoandar-ecommerce-analysis.olist_staging.stg_payments`) AS source_total_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_revenue_summary`
WHERE time_period = 'monthly';

-- 3. New vs repeat customer analysis
SELECT 
  time_period,
  ROUND(SUM(new_customer_revenue), 2) AS total_new_revenue,
  ROUND(SUM(repeat_customer_revenue), 2) AS total_repeat_revenue,
  ROUND(AVG(repeat_revenue_rate), 2) AS avg_repeat_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_revenue_summary`
GROUP BY time_period
ORDER BY time_period;

-- 4. Monthly growth trends analysis
SELECT 
  revenue_date,
  ROUND(total_revenue, 2) AS monthly_revenue,
  ROUND(revenue_growth_mom_pct, 2) AS mom_growth_pct,
  ROUND(repeat_revenue_rate, 2) AS repeat_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_revenue_summary`
WHERE time_period = 'monthly'
ORDER BY revenue_date DESC
LIMIT 12;

-- 5. Data quality checks
SELECT 
  COUNT(*) AS total_rows,
  COUNTIF(total_revenue IS NULL) AS null_revenue_rows,
  COUNTIF(total_customers IS NULL) AS null_customer_rows,
  COUNTIF(revenue_date IS NULL) AS null_date_rows
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_revenue_summary`;

-- 6. Top performing periods
SELECT 
  revenue_date,
  time_period,
  ROUND(total_revenue, 2) AS revenue,
  total_orders,
  ROUND(repeat_revenue_rate, 2) AS repeat_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_revenue_summary`
WHERE time_period = 'monthly'
ORDER BY total_revenue DESC
LIMIT 10;
*/