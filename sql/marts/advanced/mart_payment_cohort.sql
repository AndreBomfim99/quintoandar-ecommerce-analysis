-- =========================================================
-- MART: PAYMENT METHOD COHORTS
-- =========================================================
-- Description: Customer behavior analysis grouped by initial payment method
-- Sources: stg_payments, stg_orders, stg_customers
-- Destination: olist_marts.mart_payment_cohort
-- Granularity: 1 row per customer with payment cohort analysis
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_payment_cohort` AS

WITH first_payment_method AS (
  SELECT
    o.customer_id,
    p.payment_type AS first_payment_method,
    p.payment_installments AS first_payment_installments,
    o.order_purchase_timestamp AS first_purchase_date,
    p.payment_value AS first_purchase_value
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  WHERE o.is_completed = TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY o.customer_id 
    ORDER BY o.order_purchase_timestamp
  ) = 1
),

payment_method_frequency AS (
  SELECT
    o.customer_id,
    p.payment_type,
    COUNT(*) as payment_count
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  WHERE o.is_completed = TRUE
  GROUP BY o.customer_id, p.payment_type
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY o.customer_id 
    ORDER BY COUNT(*) DESC
  ) = 1
),

customer_payment_behavior AS (
  SELECT
    fpm.customer_id,
    fpm.first_payment_method,
    fpm.first_payment_installments,
    fpm.first_purchase_date,
    fpm.first_purchase_value,
    
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT p.payment_type) AS unique_payment_methods_used,
    
    CASE 
      WHEN COUNT(DISTINCT o.order_id) > 1 THEN
        COUNT(DISTINCT CASE WHEN p.payment_type = fpm.first_payment_method THEN o.order_id END) / COUNT(DISTINCT o.order_id)
      ELSE 1
    END AS payment_method_consistency,
    
    CASE 
      WHEN COUNT(DISTINCT o.order_id) > 1 AND COUNT(DISTINCT p.payment_type) > 1 THEN 1
      ELSE 0
    END AS has_payment_method_migration,
    
    pmf.payment_type AS most_frequent_payment_method,
    
    AVG(p.payment_installments) AS avg_installments_all_orders,
    MAX(p.payment_installments) AS max_installments_used

  FROM first_payment_method fpm
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
    ON fpm.customer_id = o.customer_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  LEFT JOIN payment_method_frequency pmf
    ON fpm.customer_id = pmf.customer_id
  WHERE o.is_completed = TRUE
  GROUP BY 
    fpm.customer_id, fpm.first_payment_method, fpm.first_payment_installments,
    fpm.first_purchase_date, fpm.first_purchase_value, pmf.payment_type
),

customer_revenue_metrics AS (
  SELECT
    cpb.customer_id,
    cpb.first_payment_method,
    
    SUM(p.payment_value) AS total_revenue,
    AVG(p.payment_value) AS avg_order_value,
    MAX(p.payment_value) AS max_order_value,
    
    DATE_DIFF(MAX(o.order_purchase_timestamp), MIN(o.order_purchase_timestamp), DAY) AS customer_lifetime_days,
    COUNT(DISTINCT o.order_id) AS order_frequency,
    
    CASE WHEN COUNT(DISTINCT o.order_id) > 1 THEN 1 ELSE 0 END AS is_repeat_customer,
    COUNT(DISTINCT o.order_id) - 1 AS repeat_order_count

  FROM customer_payment_behavior cpb
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
    ON cpb.customer_id = o.customer_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_payments` p
    ON o.order_id = p.order_id
  WHERE o.is_completed = TRUE
  GROUP BY cpb.customer_id, cpb.first_payment_method
),

payment_migration_analysis AS (
  SELECT
    cpb.customer_id,
    
    CASE 
      WHEN cpb.has_payment_method_migration = 1 THEN
        CASE 
          WHEN cpb.most_frequent_payment_method != cpb.first_payment_method THEN 'Changed_Primary_Method'
          ELSE 'Added_Secondary_Methods'
        END
      ELSE 'No_Migration'
    END AS migration_pattern,
    
    CASE 
      WHEN cpb.has_payment_method_migration = 1 THEN
        CONCAT(cpb.first_payment_method, '_to_', cpb.most_frequent_payment_method)
      ELSE CONCAT(cpb.first_payment_method, '_stable')
    END AS migration_path,
    
    cpb.unique_payment_methods_used AS payment_method_diversity

  FROM customer_payment_behavior cpb
),

final_payment_cohort AS (
  SELECT
    cpb.customer_id,
    c.first_purchase_date,
    
    cpb.first_payment_method AS cohort_payment,
    cpb.first_payment_installments,
    cpb.first_purchase_value,
    
    cpb.total_orders,
    cpb.unique_payment_methods_used,
    cpb.payment_method_consistency,
    cpb.has_payment_method_migration,
    cpb.most_frequent_payment_method,
    cpb.avg_installments_all_orders,
    cpb.max_installments_used,
    
    crm.total_revenue,
    crm.avg_order_value,
    crm.max_order_value,
    crm.customer_lifetime_days,
    crm.order_frequency,
    crm.is_repeat_customer,
    crm.repeat_order_count,
    
    pma.migration_pattern,
    pma.migration_path,
    pma.payment_method_diversity,
    
    crm.total_revenue / NULLIF(crm.order_frequency, 0) AS revenue_per_order,
    crm.total_revenue / NULLIF(DATE_DIFF(CURRENT_DATE(), DATE(c.first_purchase_date), DAY), 0) * 30 AS estimated_monthly_ltv,
    
    CASE
      WHEN crm.total_revenue > 1000 THEN 'High_Value'
      WHEN crm.total_revenue > 500 THEN 'Medium_Value'
      WHEN crm.total_revenue > 100 THEN 'Low_Value'
      ELSE 'Minimal_Value'
    END AS customer_value_segment

  FROM customer_payment_behavior cpb
  INNER JOIN first_payment_method c ON cpb.customer_id = c.customer_id
  INNER JOIN customer_revenue_metrics crm ON cpb.customer_id = crm.customer_id
  LEFT JOIN payment_migration_analysis pma ON cpb.customer_id = pma.customer_id
)

SELECT *
FROM final_payment_cohort
ORDER BY cohort_payment, total_revenue DESC;


/*
-- VALIDATION QUERIES
-- 1. LTV by initial payment method
SELECT
  cohort_payment,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(AVG(avg_order_value), 2) AS avg_aov,
  ROUND(AVG(order_frequency), 2) AS avg_orders_per_customer,
  ROUND(SUM(is_repeat_customer) * 100.0 / COUNT(*), 2) AS retention_rate_pct
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_cohort`
GROUP BY cohort_payment
ORDER BY avg_ltv DESC;

-- 2. Payment method migration analysis
SELECT
  migration_pattern,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(AVG(order_frequency), 2) AS avg_orders,
  ROUND(AVG(payment_method_consistency) * 100, 2) AS avg_consistency_pct
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_cohort`
GROUP BY migration_pattern
ORDER BY customer_count DESC;

-- 3. First purchase installments analysis
SELECT
  cohort_payment,
  ROUND(AVG(first_payment_installments), 2) AS avg_initial_installments,
  ROUND(AVG(first_purchase_value), 2) AS avg_initial_order_value,
  ROUND(AVG(avg_installments_all_orders), 2) AS avg_lifetime_installments,
  ROUND(AVG(max_installments_used), 2) AS max_installments_used
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_cohort`
GROUP BY cohort_payment
ORDER BY avg_initial_installments DESC;

-- 4. Specific migration paths analysis
SELECT
  migration_path,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(AVG(order_frequency), 2) AS avg_orders,
  ROUND(AVG(payment_method_diversity), 2) AS avg_method_diversity
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_cohort`
WHERE has_payment_method_migration = 1
GROUP BY migration_path
ORDER BY customer_count DESC
LIMIT 10;

-- 5. Payment method consistency vs LTV
SELECT
  CASE
    WHEN payment_method_consistency = 1 THEN 'Always_Same_Method'
    WHEN payment_method_consistency >= 0.7 THEN 'Mostly_Consistent'
    WHEN payment_method_consistency >= 0.4 THEN 'Mixed_Methods'
    ELSE 'Highly_Variable'
  END AS consistency_segment,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(AVG(order_frequency), 2) AS avg_orders,
  ROUND(AVG(avg_order_value), 2) AS avg_aov
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_cohort`
GROUP BY consistency_segment
ORDER BY avg_ltv DESC;

-- 6. Customer value segmentation by payment cohort
SELECT
  cohort_payment,
  customer_value_segment,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(AVG(order_frequency), 2) AS avg_orders,
  ROUND(SUM(is_repeat_customer) * 100.0 / COUNT(*), 2) AS retention_rate_pct
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_cohort`
GROUP BY cohort_payment, customer_value_segment
ORDER BY cohort_payment, avg_ltv DESC;

-- 7. Time-based analysis of payment behavior
SELECT
  cohort_payment,
  EXTRACT(YEAR FROM first_purchase_date) AS cohort_year,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(AVG(customer_lifetime_days), 1) AS avg_lifetime_days,
  ROUND(AVG(order_frequency), 2) AS avg_orders
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_cohort`
GROUP BY cohort_payment, cohort_year
ORDER BY cohort_year DESC, avg_ltv DESC;

-- 8. High-value customer analysis by payment method
SELECT
  cohort_payment,
  COUNT(*) AS total_customers,
  COUNTIF(customer_value_segment = 'High_Value') AS high_value_customers,
  ROUND(COUNTIF(customer_value_segment = 'High_Value') * 100.0 / COUNT(*), 2) AS high_value_pct,
  ROUND(AVG(CASE WHEN customer_value_segment = 'High_Value' THEN total_revenue END), 2) AS avg_high_value_ltv
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_payment_cohort`
GROUP BY cohort_payment
ORDER BY high_value_pct DESC;
*/