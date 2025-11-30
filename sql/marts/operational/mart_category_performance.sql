-- =========================================================
-- MART: CATEGORY PERFORMANCE
-- =========================================================
-- Description: Product category sales performance analysis
-- Based on: Analysis #12 (Category Performance)
-- Source: stg_order_items, stg_products, stg_orders
-- Destination: olist_marts.mart_category_performance
-- Granularity: 1 row per category
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_category_performance` AS

WITH category_sales AS (

  SELECT
    p.product_category_name_english AS category,
    
    ROUND(SUM(oi.price), 2) AS category_revenue,
    COUNT(DISTINCT oi.order_id) AS category_orders,
    COUNT(*) AS category_units,
    
    ROUND(AVG(oi.price), 2) AS avg_price_per_category,
    ROUND(MIN(oi.price), 2) AS min_price,
    ROUND(MAX(oi.price), 2) AS max_price,
    
    ROUND(SUM(oi.freight_value), 2) AS total_freight,
    ROUND(AVG(oi.freight_value), 2) AS avg_freight,
    
    ROUND(SUM(oi.price - oi.freight_value), 2) AS category_margin,
    ROUND(AVG((oi.price - oi.freight_value) / NULLIF(oi.price, 0)) * 100, 2) AS avg_margin_pct,
    
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    COUNT(DISTINCT oi.product_id) AS unique_products
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` p
    ON oi.product_id = p.product_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
    ON oi.order_id = o.order_id
  WHERE (o.is_delivered OR o.is_completed)
    AND p.product_category_name_english IS NOT NULL
  GROUP BY p.product_category_name_english
),

category_max_dates AS (
  SELECT
    p.product_category_name_english AS category,
    MAX(o.order_purchase_timestamp) AS max_order_date
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
    ON o.order_id = oi.order_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` p
    ON oi.product_id = p.product_id
  WHERE (o.is_delivered OR o.is_completed)
    AND p.product_category_name_english IS NOT NULL
  GROUP BY p.product_category_name_english
),

category_temporal AS (
  SELECT
    p.product_category_name_english AS category,
    
    MIN(o.order_purchase_timestamp) AS first_sale_date,
    MAX(o.order_purchase_timestamp) AS last_sale_date,
    
    COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', o.order_purchase_timestamp)) AS active_months,
    
    ROUND(SUM(CASE 
      WHEN DATE(o.order_purchase_timestamp) >= DATE_SUB(DATE(cmd.max_order_date), INTERVAL 3 MONTH)
      THEN oi.price ELSE 0 END), 2) AS revenue_last_3m,
    
    ROUND(SUM(CASE 
      WHEN DATE(o.order_purchase_timestamp) >= DATE_SUB(DATE(cmd.max_order_date), INTERVAL 6 MONTH)
           AND DATE(o.order_purchase_timestamp) < DATE_SUB(DATE(cmd.max_order_date), INTERVAL 3 MONTH)
      THEN oi.price ELSE 0 END), 2) AS revenue_prev_3m
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` p
    ON oi.product_id = p.product_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
    ON oi.order_id = o.order_id
  INNER JOIN category_max_dates cmd
    ON p.product_category_name_english = cmd.category
  WHERE (o.is_delivered OR o.is_completed)
    AND p.product_category_name_english IS NOT NULL
  GROUP BY p.product_category_name_english
),

category_repeat_behavior AS (
  SELECT
    category,
    COUNT(DISTINCT customer_id) AS total_customers_in_category,
    COUNT(DISTINCT CASE WHEN purchase_count >= 2 THEN customer_id END) AS repeat_customers,
    ROUND(COUNT(DISTINCT CASE WHEN purchase_count >= 2 THEN customer_id END) * 100.0 / 
          NULLIF(COUNT(DISTINCT customer_id), 0), 2) AS repeat_purchase_rate_category
  FROM (
    SELECT
      p.product_category_name_english AS category,
      o.customer_id,
      COUNT(DISTINCT oi.order_id) AS purchase_count
    FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
    INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` p
      ON oi.product_id = p.product_id
    INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
      ON oi.order_id = o.order_id
    WHERE (o.is_delivered OR o.is_completed)
      AND p.product_category_name_english IS NOT NULL
    GROUP BY p.product_category_name_english, o.customer_id
  )
  GROUP BY category
),

category_cross_purchase AS (
  SELECT
    p.product_category_name_english AS category,
    COUNT(DISTINCT CASE 
      WHEN customer_categories > 1 THEN o.customer_id 
    END) AS cross_category_customers,
    ROUND(COUNT(DISTINCT CASE 
      WHEN customer_categories > 1 THEN o.customer_id 
    END) * 100.0 / NULLIF(COUNT(DISTINCT o.customer_id), 0), 2) AS cross_category_rate
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` p
    ON oi.product_id = p.product_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
    ON oi.order_id = o.order_id
  INNER JOIN (
    SELECT
      o2.customer_id,
      COUNT(DISTINCT p2.product_category_name_english) AS customer_categories
    FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi2
    INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` p2
      ON oi2.product_id = p2.product_id
    INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o2
      ON oi2.order_id = o2.order_id
    WHERE (o2.is_delivered OR o2.is_completed)
      AND p2.product_category_name_english IS NOT NULL
    GROUP BY o2.customer_id
  ) customer_cat_count
    ON o.customer_id = customer_cat_count.customer_id
  WHERE (o.is_delivered OR o.is_completed)
    AND p.product_category_name_english IS NOT NULL
  GROUP BY p.product_category_name_english
),

category_with_share AS (
  SELECT
    cs.*,
    
    ROUND(cs.category_revenue * 100.0 / SUM(cs.category_revenue) OVER (), 2) AS category_share,
    
    ROUND(SUM(cs.category_revenue) OVER (ORDER BY cs.category_revenue DESC) * 100.0 / 
          SUM(cs.category_revenue) OVER (), 2) AS cumulative_share,
    
    DENSE_RANK() OVER (ORDER BY cs.category_revenue DESC) AS revenue_rank
    
  FROM category_sales cs
),

final_combined AS (
  SELECT
    cws.category,
    cws.category_revenue,
    cws.category_orders,
    cws.category_units,
    cws.avg_price_per_category,
    cws.min_price,
    cws.max_price,
    cws.total_freight,
    cws.avg_freight,
    cws.category_margin,
    cws.avg_margin_pct,
    cws.unique_customers,
    cws.unique_products,
    cws.category_share,
    cws.cumulative_share,
    cws.revenue_rank,
    
    ct.first_sale_date,
    ct.last_sale_date,
    ct.active_months,
    ct.revenue_last_3m,
    ct.revenue_prev_3m,
    
    ROUND(
      CASE 
        WHEN ct.revenue_prev_3m > 0 
        THEN ((ct.revenue_last_3m - ct.revenue_prev_3m) / ct.revenue_prev_3m) * 100
        ELSE NULL
      END, 
      2
    ) AS category_growth_pct,
    
    COALESCE(crb.repeat_purchase_rate_category, 0) AS repeat_purchase_rate_category,
    COALESCE(crb.repeat_customers, 0) AS repeat_customers,
    
    COALESCE(ccp.cross_category_rate, 0) AS cross_category_rate,
    COALESCE(ccp.cross_category_customers, 0) AS cross_category_customers,
    
    ROUND(cws.category_revenue / NULLIF(cws.unique_customers, 0), 2) AS revenue_per_customer,
    ROUND(cws.category_orders / NULLIF(cws.unique_customers, 0), 2) AS orders_per_customer,
    ROUND(cws.category_units / NULLIF(cws.category_orders, 0), 2) AS units_per_order
    
  FROM category_with_share cws
  LEFT JOIN category_temporal ct
    ON cws.category = ct.category
  LEFT JOIN category_repeat_behavior crb
    ON cws.category = crb.category
  LEFT JOIN category_cross_purchase ccp
    ON cws.category = ccp.category
)

SELECT *
FROM final_combined
ORDER BY revenue_rank;


/*
-- VALIDATION QUERIES
-- 1. Top 10 categories by revenue
SELECT
  category,
  category_revenue,
  category_share,
  cumulative_share,
  category_orders,
  category_units,
  avg_price_per_category
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_category_performance`
ORDER BY category_revenue DESC
LIMIT 10;

-- 2. Top 10 categories by volume (units)
SELECT
  category,
  category_units,
  category_orders,
  category_revenue,
  avg_price_per_category
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_category_performance`
ORDER BY category_units DESC
LIMIT 10;

-- 3. Category growth analysis (fastest growing)
SELECT
  category,
  revenue_last_3m,
  revenue_prev_3m,
  category_growth_pct,
  category_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_category_performance`
WHERE revenue_prev_3m > 0
ORDER BY category_growth_pct DESC
LIMIT 10;

-- 4. Margin analysis by category
SELECT
  category,
  category_revenue,
  category_margin,
  avg_margin_pct,
  avg_freight,
  CASE
    WHEN avg_margin_pct >= 80 THEN 'High Margin (80%+)'
    WHEN avg_margin_pct >= 70 THEN 'Good Margin (70-80%)'
    WHEN avg_margin_pct >= 60 THEN 'Average Margin (60-70%)'
    ELSE 'Low Margin (<60%)'
  END AS margin_category
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_category_performance`
ORDER BY avg_margin_pct DESC;

-- 5. Repeat purchase rate by category
SELECT
  category,
  unique_customers,
  repeat_customers,
  repeat_purchase_rate_category,
  category_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_category_performance`
ORDER BY repeat_purchase_rate_category DESC
LIMIT 15;

-- 6. Cross-category purchase behavior
SELECT
  category,
  unique_customers,
  cross_category_customers,
  cross_category_rate,
  category_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_category_performance`
ORDER BY cross_category_rate DESC
LIMIT 15;

-- 7. Revenue concentration (Pareto analysis)
SELECT
  CASE
    WHEN cumulative_share <= 80 THEN 'Top 80% Revenue'
    ELSE 'Bottom 20% Revenue'
  END AS pareto_group,
  COUNT(*) AS num_categories,
  ROUND(SUM(category_revenue), 2) AS total_revenue,
  ROUND(SUM(category_share), 2) AS total_share
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_category_performance`
GROUP BY pareto_group;

-- 8. Price segment analysis
SELECT
  CASE
    WHEN avg_price_per_category < 50 THEN 'Budget (<R$50)'
    WHEN avg_price_per_category < 100 THEN 'Economy (R$50-100)'
    WHEN avg_price_per_category < 200 THEN 'Mid-Range (R$100-200)'
    ELSE 'Premium (R$200+)'
  END AS price_segment,
  COUNT(*) AS num_categories,
  ROUND(SUM(category_revenue), 2) AS total_revenue,
  ROUND(AVG(avg_price_per_category), 2) AS avg_price,
  ROUND(AVG(category_units), 0) AS avg_units
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_category_performance`
GROUP BY price_segment
ORDER BY 
  CASE price_segment
    WHEN 'Budget (<R$50)' THEN 1
    WHEN 'Economy (R$50-100)' THEN 2
    WHEN 'Mid-Range (R$100-200)' THEN 3
    WHEN 'Premium (R$200+)' THEN 4
  END;

-- 9. Customer engagement by category
SELECT
  category,
  unique_customers,
  revenue_per_customer,
  orders_per_customer,
  units_per_order,
  category_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_category_performance`
ORDER BY revenue_per_customer DESC
LIMIT 15;

-- 10. Category performance summary
SELECT
  COUNT(*) AS total_categories,
  ROUND(SUM(category_revenue), 2) AS total_revenue,
  ROUND(AVG(category_revenue), 2) AS avg_revenue_per_category,
  ROUND(AVG(avg_price_per_category), 2) AS overall_avg_price,
  ROUND(AVG(avg_margin_pct), 2) AS overall_avg_margin,
  ROUND(AVG(repeat_purchase_rate_category), 2) AS overall_repeat_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_category_performance`;
*/