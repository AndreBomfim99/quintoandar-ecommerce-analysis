-- =========================================================
-- MART: CUSTOMER AGE ANALYSIS
-- =========================================================
-- Description: Customer behavior evolution by lifecycle stage
-- Based on: Analysis #24 (Customer Age Analysis)
-- Source: mart_customer_base, stg_orders, stg_payments
-- Destination: olist_marts.mart_customer_age
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_customer_age` AS

WITH customer_age_base AS (
  
  SELECT
    cb.customer_id,
    cb.customer_state,
    cb.customer_city,
    cb.customer_region,
    
    cb.first_purchase_date,
    cb.last_purchase_date,
    cb.customer_lifespan_days,
    
    CURRENT_DATE() AS analysis_date,
    DATE_DIFF(CURRENT_DATE(), DATE(cb.first_purchase_date), DAY) AS customer_age_days,
    ROUND(DATE_DIFF(CURRENT_DATE(), DATE(cb.first_purchase_date), DAY) / 30.0, 0) AS customer_age_months,
    
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(cb.first_purchase_date), MONTH) <= 3 THEN '0-3m'
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(cb.first_purchase_date), MONTH) <= 6 THEN '3-6m'
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(cb.first_purchase_date), MONTH) <= 12 THEN '6-12m'
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(cb.first_purchase_date), MONTH) <= 18 THEN '12-18m'
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(cb.first_purchase_date), MONTH) <= 24 THEN '18-24m'
      ELSE '24+m'
    END AS customer_age_bin,
    
    cb.total_orders,
    cb.total_revenue,
    cb.avg_order_value,
    cb.is_repeat_customer,
    cb.is_churned,
    cb.recency_days,
    cb.avg_review_score,
    cb.avg_delivery_days
    
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_base` cb
  WHERE cb.first_purchase_date IS NOT NULL
),

customer_order_metrics AS (
  
  SELECT
    o.customer_id,
    COUNT(DISTINCT o.order_id) AS total_orders_calc,
    SUM(p.payment_value) AS total_revenue_calc,
    AVG(p.payment_value) AS avg_order_value_calc,
    MIN(o.order_purchase_timestamp) AS first_order_timestamp,
    MAX(o.order_purchase_timestamp) AS last_order_timestamp
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  WHERE o.is_completed = TRUE
  GROUP BY o.customer_id
),

age_bin_aggregations AS (
  
  SELECT
    customer_age_bin,
  
    COUNT(DISTINCT customer_id) AS customers_in_bin,
    
    ROUND(AVG(total_orders), 2) AS avg_orders_in_bin,
    SUM(total_orders) AS total_orders_in_bin,
    
    ROUND(AVG(total_revenue), 2) AS avg_revenue_in_bin,
    ROUND(SUM(total_revenue), 2) AS total_revenue_in_bin,
    
    ROUND(AVG(avg_order_value), 2) AS avg_aov_in_bin,
    
    ROUND(COUNTIF(is_churned) * 100.0 / COUNT(*), 2) AS churn_rate_bin,
    ROUND(COUNTIF(NOT is_churned) * 100.0 / COUNT(*), 2) AS active_rate_bin,
    
    ROUND(COUNTIF(is_repeat_customer) * 100.0 / COUNT(*), 2) AS repeat_rate_bin,
    
    ROUND(AVG(customer_age_days), 1) AS avg_age_days_in_bin,
    ROUND(AVG(customer_age_months), 1) AS avg_age_months_in_bin
    
  FROM customer_age_base
  GROUP BY customer_age_bin
),

cumulative_metrics AS (
  
  SELECT
    customer_age_bin,
    customers_in_bin,
    avg_orders_in_bin,
    avg_revenue_in_bin,
    avg_aov_in_bin,
    churn_rate_bin,
    active_rate_bin,
    repeat_rate_bin,
    avg_age_days_in_bin,
    avg_age_months_in_bin,
    
    SUM(total_revenue_in_bin) OVER (
      ORDER BY 
        CASE customer_age_bin
          WHEN '0-3m' THEN 1
          WHEN '3-6m' THEN 2
          WHEN '6-12m' THEN 3
          WHEN '12-18m' THEN 4
          WHEN '18-24m' THEN 5
          WHEN '24+m' THEN 6
        END
    ) AS cumulative_revenue,
    
    SUM(total_orders_in_bin) OVER (
      ORDER BY 
        CASE customer_age_bin
          WHEN '0-3m' THEN 1
          WHEN '3-6m' THEN 2
          WHEN '6-12m' THEN 3
          WHEN '12-18m' THEN 4
          WHEN '18-24m' THEN 5
          WHEN '24+m' THEN 6
        END
    ) AS cumulative_orders,
    
    SUM(customers_in_bin) OVER (
      ORDER BY 
        CASE customer_age_bin
          WHEN '0-3m' THEN 1
          WHEN '3-6m' THEN 2
          WHEN '6-12m' THEN 3
          WHEN '12-18m' THEN 4
          WHEN '18-24m' THEN 5
          WHEN '24+m' THEN 6
        END
    ) AS cumulative_customers
    
  FROM age_bin_aggregations
),

lifecycle_flags AS (

  SELECT
    cm.*,
    
    CASE 
      WHEN customer_age_bin IN ('0-3m', '3-6m') THEN TRUE 
      ELSE FALSE 
    END AS is_honeymoon_period,
    
    CASE 
      WHEN customer_age_bin = '6-12m' THEN TRUE 
      ELSE FALSE 
    END AS is_danger_zone,
    
    CASE 
      WHEN customer_age_bin IN ('12-18m', '18-24m', '24+m') THEN TRUE 
      ELSE FALSE 
    END AS is_mature_period,
    
    ROUND(
      cm.avg_orders_in_bin / NULLIF(cm.avg_age_months_in_bin, 0), 
      3
    ) AS monthly_order_frequency,
    
    ROUND(
      cm.avg_revenue_in_bin / NULLIF(cm.avg_age_months_in_bin, 0), 
      2
    ) AS monthly_revenue_rate
    
  FROM cumulative_metrics cm
),

final_enriched AS (
  
  SELECT
    lf.*,
    
    DENSE_RANK() OVER (ORDER BY lf.avg_revenue_in_bin DESC) AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY lf.avg_orders_in_bin DESC) AS orders_rank,
    DENSE_RANK() OVER (ORDER BY lf.churn_rate_bin ASC) AS retention_rank,
    
    LAG(lf.avg_revenue_in_bin) OVER (
      ORDER BY 
        CASE lf.customer_age_bin
          WHEN '0-3m' THEN 1
          WHEN '3-6m' THEN 2
          WHEN '6-12m' THEN 3
          WHEN '12-18m' THEN 4
          WHEN '18-24m' THEN 5
          WHEN '24+m' THEN 6
        END
    ) AS prev_bin_revenue,
    
    ROUND(
      (lf.avg_revenue_in_bin - LAG(lf.avg_revenue_in_bin) OVER (
        ORDER BY 
          CASE lf.customer_age_bin
            WHEN '0-3m' THEN 1
            WHEN '3-6m' THEN 2
            WHEN '6-12m' THEN 3
            WHEN '12-18m' THEN 4
            WHEN '18-24m' THEN 5
            WHEN '24+m' THEN 6
          END
      )) / NULLIF(LAG(lf.avg_revenue_in_bin) OVER (
        ORDER BY 
          CASE lf.customer_age_bin
            WHEN '0-3m' THEN 1
            WHEN '3-6m' THEN 2
            WHEN '6-12m' THEN 3
            WHEN '12-18m' THEN 4
            WHEN '18-24m' THEN 5
            WHEN '24+m' THEN 6
          END
      ), 0) * 100,
      2
    ) AS revenue_growth_rate_pct
    
  FROM lifecycle_flags lf
)

SELECT 
  customer_age_bin,
  customers_in_bin,
  avg_orders_in_bin,
  avg_revenue_in_bin,
  avg_aov_in_bin,
  churn_rate_bin,
  active_rate_bin,
  repeat_rate_bin,
  avg_age_days_in_bin,
  avg_age_months_in_bin,
  cumulative_revenue,
  cumulative_orders,
  cumulative_customers,
  is_honeymoon_period,
  is_danger_zone,
  is_mature_period,
  monthly_order_frequency,
  monthly_revenue_rate,
  revenue_rank,
  orders_rank,
  retention_rank,
  prev_bin_revenue,
  revenue_growth_rate_pct
FROM final_enriched
ORDER BY 
  CASE customer_age_bin
    WHEN '0-3m' THEN 1
    WHEN '3-6m' THEN 2
    WHEN '6-12m' THEN 3
    WHEN '12-18m' THEN 4
    WHEN '18-24m' THEN 5
    WHEN '24+m' THEN 6
  END;


/*
-- VALIDATION QUERIES
-- 1. Customer distribution by age bins
SELECT 
  customer_age_bin,
  customers_in_bin,
  ROUND(customers_in_bin * 100.0 / SUM(customers_in_bin) OVER (), 2) AS pct_of_total,
  avg_age_days_in_bin,
  avg_orders_in_bin
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_age`
ORDER BY 
  CASE customer_age_bin
    WHEN '0-3m' THEN 1
    WHEN '3-6m' THEN 2
    WHEN '6-12m' THEN 3
    WHEN '12-18m' THEN 4
    WHEN '18-24m' THEN 5
    WHEN '24+m' THEN 6
  END;

-- 2. LTV curve by customer age
SELECT 
  customer_age_bin,
  avg_revenue_in_bin AS avg_ltv,
  cumulative_revenue,
  avg_orders_in_bin,
  avg_aov_in_bin,
  monthly_order_frequency
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_age`
ORDER BY 
  CASE customer_age_bin
    WHEN '0-3m' THEN 1
    WHEN '3-6m' THEN 2
    WHEN '6-12m' THEN 3
    WHEN '12-18m' THEN 4
    WHEN '18-24m' THEN 5
    WHEN '24+m' THEN 6
  END;

-- 3. Churn analysis by customer age
SELECT 
  customer_age_bin,
  customers_in_bin,
  churn_rate_bin,
  active_rate_bin,
  repeat_rate_bin
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_age`
ORDER BY 
  CASE customer_age_bin
    WHEN '0-3m' THEN 1
    WHEN '3-6m' THEN 2
    WHEN '6-12m' THEN 3
    WHEN '12-18m' THEN 4
    WHEN '18-24m' THEN 5
    WHEN '24+m' THEN 6
  END;

-- 4. Honeymoon period vs Danger zone analysis
SELECT 
  CASE 
    WHEN is_honeymoon_period THEN 'Honeymoon Period (0-6m)'
    WHEN is_danger_zone THEN 'Danger Zone (6-12m)'
    WHEN is_mature_period THEN 'Mature Period (12+m)'
  END AS lifecycle_stage,
  SUM(customers_in_bin) AS total_customers,
  ROUND(AVG(avg_orders_in_bin), 2) AS avg_orders,
  ROUND(AVG(avg_revenue_in_bin), 2) AS avg_revenue,
  ROUND(AVG(avg_aov_in_bin), 2) AS avg_aov,
  ROUND(AVG(churn_rate_bin), 2) AS avg_churn_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_age`
GROUP BY lifecycle_stage
ORDER BY lifecycle_stage;

-- 5. Purchase frequency evolution by customer age
SELECT 
  customer_age_bin,
  monthly_order_frequency,
  avg_orders_in_bin,
  revenue_growth_rate_pct
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_age`
ORDER BY 
  CASE customer_age_bin
    WHEN '0-3m' THEN 1
    WHEN '3-6m' THEN 2
    WHEN '6-12m' THEN 3
    WHEN '12-18m' THEN 4
    WHEN '18-24m' THEN 5
    WHEN '24+m' THEN 6
  END;

-- 6. Best performing age bins (by revenue)
SELECT 
  customer_age_bin,
  avg_revenue_in_bin,
  revenue_rank,
  orders_rank,
  retention_rank
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_age`
ORDER BY revenue_rank;

-- 7. Cumulative metrics progression
SELECT 
  customer_age_bin,
  cumulative_customers,
  ROUND(cumulative_revenue, 2) AS cumulative_revenue,
  cumulative_orders,
  ROUND(cumulative_revenue / NULLIF(cumulative_customers, 0), 2) AS avg_ltv_cumulative
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_customer_age`
ORDER BY 
  CASE customer_age_bin
    WHEN '0-3m' THEN 1
    WHEN '3-6m' THEN 2
    WHEN '6-12m' THEN 3
    WHEN '12-18m' THEN 4
    WHEN '18-24m' THEN 5
    WHEN '24+m' THEN 6
  END;
*/