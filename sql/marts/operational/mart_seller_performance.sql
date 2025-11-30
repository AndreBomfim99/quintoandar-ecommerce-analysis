-- =========================================================
-- MART: SELLER PERFORMANCE
-- =========================================================
-- Description: Marketplace seller performance analysis
-- Based on: Analysis #16 (Seller Performance)
-- Source: stg_sellers, stg_order_items, stg_orders, stg_reviews, stg_products
-- Destination: olist_marts.mart_seller_performance
-- Granularity: 1 row per seller
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance` AS

WITH seller_sales_metrics AS (
  
  SELECT
    oi.seller_id,
    
    ROUND(SUM(oi.price), 2) AS seller_revenue,
    COUNT(DISTINCT oi.order_id) AS seller_orders,
    COUNT(*) AS seller_units,
    
    ROUND(AVG(oi.price), 2) AS seller_avg_price,
    ROUND(MIN(oi.price), 2) AS seller_min_price,
    ROUND(MAX(oi.price), 2) AS seller_max_price,

    ROUND(AVG(oi.freight_value), 2) AS seller_avg_freight,
    
    COUNT(DISTINCT oi.product_id) AS seller_products_sold,
    
    MIN(o.order_purchase_timestamp) AS first_sale_date,
    MAX(o.order_purchase_timestamp) AS last_sale_date,
    COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', o.order_purchase_timestamp)) AS seller_active_months
    
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
    ON oi.order_id = o.order_id
  WHERE o.is_delivered OR o.is_completed
  GROUP BY oi.seller_id
),

seller_category_diversity AS (

  SELECT
    oi.seller_id,
    COUNT(DISTINCT p.product_category_name_english) AS seller_categories,
    ARRAY_AGG(p.product_category_name_english ORDER BY item_count DESC LIMIT 1)[OFFSET(0)] AS primary_category
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` p
    ON oi.product_id = p.product_id
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o
    ON oi.order_id = o.order_id
  INNER JOIN (
    SELECT
      oi2.seller_id,
      p2.product_category_name_english,
      COUNT(*) AS item_count
    FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi2
    INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_products` p2
      ON oi2.product_id = p2.product_id
    INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_orders` o2
      ON oi2.order_id = o2.order_id
    WHERE (o2.is_delivered OR o2.is_completed)
      AND p2.product_category_name_english IS NOT NULL
    GROUP BY oi2.seller_id, p2.product_category_name_english
  ) cat_counts
    ON oi.seller_id = cat_counts.seller_id 
    AND p.product_category_name_english = cat_counts.product_category_name_english
  WHERE (o.is_delivered OR o.is_completed)
    AND p.product_category_name_english IS NOT NULL
  GROUP BY oi.seller_id
),

seller_review_metrics AS (
  SELECT
    oi.seller_id,
    COUNT(DISTINCT r.review_id) AS seller_total_reviews,
    ROUND(AVG(r.review_score), 2) AS seller_avg_review,
    COUNTIF(r.review_score >= 4) AS positive_reviews,
    COUNTIF(r.review_score <= 2) AS negative_reviews,
    ROUND(COUNTIF(r.review_score >= 4) * 100.0 / COUNT(*), 2) AS positive_review_rate
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_reviews` r
    ON oi.order_id = r.order_id
  GROUP BY oi.seller_id
),

seller_delivery_metrics AS (
  SELECT
    oi.seller_id,
    COUNT(*) AS total_deliveries,
    COUNTIF(NOT dp.is_delayed) AS on_time_deliveries,
    ROUND(COUNTIF(NOT dp.is_delayed) * 100.0 / COUNT(*), 2) AS seller_delivery_performance,
    ROUND(AVG(dp.delivery_time_actual), 1) AS avg_delivery_days,
    ROUND(AVG(CASE WHEN dp.is_delayed THEN dp.delivery_delay END), 1) AS avg_delay_when_late
  FROM `quintoandar-ecommerce-analysis.olist_staging.stg_order_items` oi
  INNER JOIN `quintoandar-ecommerce-analysis.olist_marts.mart_delivery_performance` dp
    ON oi.order_id = dp.order_id
  WHERE oi.seller_id = dp.seller_id
  GROUP BY oi.seller_id
),

seller_with_location AS (
  SELECT
    ssm.*,
    s.seller_state,
    s.seller_city,
    s.seller_region
  FROM seller_sales_metrics ssm
  INNER JOIN `quintoandar-ecommerce-analysis.olist_staging.stg_sellers` s
    ON ssm.seller_id = s.seller_id
),

seller_with_share AS (
  SELECT
    swl.*,

    ROUND(swl.seller_revenue * 100.0 / SUM(swl.seller_revenue) OVER (), 4) AS revenue_share_pct,
    
    ROUND(SUM(swl.seller_revenue) OVER (ORDER BY swl.seller_revenue DESC) * 100.0 / 
          SUM(swl.seller_revenue) OVER (), 2) AS cumulative_revenue_share,
    
    DENSE_RANK() OVER (ORDER BY swl.seller_revenue DESC) AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY swl.seller_orders DESC) AS order_rank
    
  FROM seller_with_location swl
),

final_combined AS (
  SELECT
    sws.seller_id,
    sws.seller_state,
    sws.seller_city,
    sws.seller_region,
    sws.seller_revenue,
    sws.seller_orders,
    sws.seller_units,
    sws.seller_avg_price,
    sws.seller_min_price,
    sws.seller_max_price,
    sws.seller_avg_freight,
    sws.seller_products_sold,
    sws.first_sale_date,
    sws.last_sale_date,
    sws.seller_active_months,
    sws.revenue_share_pct,
    sws.cumulative_revenue_share,
    sws.revenue_rank,
    sws.order_rank,
    
    COALESCE(scd.seller_categories, 0) AS seller_categories,
    COALESCE(scd.primary_category, 'unknown') AS primary_category,
    
    COALESCE(srm.seller_total_reviews, 0) AS seller_total_reviews,
    COALESCE(srm.seller_avg_review, 0) AS seller_avg_review,
    COALESCE(srm.positive_reviews, 0) AS positive_reviews,
    COALESCE(srm.negative_reviews, 0) AS negative_reviews,
    COALESCE(srm.positive_review_rate, 0) AS positive_review_rate,
    
    COALESCE(sdm.seller_delivery_performance, 0) AS seller_delivery_performance,
    COALESCE(sdm.avg_delivery_days, 0) AS avg_delivery_days,
    COALESCE(sdm.avg_delay_when_late, 0) AS avg_delay_when_late,
    
    ROUND(sws.seller_revenue / NULLIF(sws.seller_active_months, 0), 2) AS revenue_per_month,
    ROUND(sws.seller_orders / NULLIF(sws.seller_active_months, 0), 2) AS orders_per_month,
    ROUND(sws.seller_units / NULLIF(sws.seller_orders, 0), 2) AS units_per_order,
    
    CASE
      WHEN sws.revenue_rank <= 10 THEN 'Top 10 Seller'
      WHEN sws.cumulative_revenue_share <= 20 THEN 'Top 20% Revenue'
      WHEN sws.cumulative_revenue_share <= 50 THEN 'Top 50% Revenue'
      WHEN sws.cumulative_revenue_share <= 80 THEN 'Top 80% Revenue'
      ELSE 'Bottom 20% Revenue'
    END AS seller_tier
    
  FROM seller_with_share sws
  LEFT JOIN seller_category_diversity scd
    ON sws.seller_id = scd.seller_id
  LEFT JOIN seller_review_metrics srm
    ON sws.seller_id = srm.seller_id
  LEFT JOIN seller_delivery_metrics sdm
    ON sws.seller_id = sdm.seller_id
)

SELECT *
FROM final_combined
ORDER BY revenue_rank;


/*
-- VALIDATION QUERIES
-- 1. Top 20 sellers by revenue
SELECT
  seller_id,
  seller_state,
  seller_revenue,
  revenue_share_pct,
  seller_orders,
  seller_avg_review,
  seller_delivery_performance,
  revenue_rank
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance`
ORDER BY seller_revenue DESC
LIMIT 20;

-- 2. Top sellers by volume (orders)
SELECT
  seller_id,
  seller_state,
  seller_orders,
  seller_units,
  seller_revenue,
  seller_avg_price,
  order_rank
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance`
ORDER BY seller_orders DESC
LIMIT 20;

-- 3. Seller distribution (Pareto analysis)
SELECT
  seller_tier,
  COUNT(*) AS num_sellers,
  ROUND(SUM(seller_revenue), 2) AS total_revenue,
  ROUND(SUM(revenue_share_pct), 2) AS revenue_share,
  ROUND(AVG(seller_avg_review), 2) AS avg_review_score
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance`
GROUP BY seller_tier
ORDER BY 
  CASE seller_tier
    WHEN 'Top 10 Seller' THEN 1
    WHEN 'Top 20% Revenue' THEN 2
    WHEN 'Top 50% Revenue' THEN 3
    WHEN 'Top 80% Revenue' THEN 4
    WHEN 'Bottom 20% Revenue' THEN 5
  END;

-- 4. Correlation: Review score vs Revenue
SELECT
  CASE
    WHEN seller_avg_review >= 4.5 THEN 'Excellent (4.5+)'
    WHEN seller_avg_review >= 4.0 THEN 'Good (4.0-4.5)'
    WHEN seller_avg_review >= 3.5 THEN 'Average (3.5-4.0)'
    ELSE 'Below Average (<3.5)'
  END AS review_category,
  COUNT(*) AS num_sellers,
  ROUND(AVG(seller_revenue), 2) AS avg_revenue,
  ROUND(AVG(seller_orders), 1) AS avg_orders,
  ROUND(AVG(seller_avg_review), 2) AS avg_review
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance`
WHERE seller_total_reviews >= 5  -- Minimum reviews for reliability
GROUP BY review_category
ORDER BY avg_review DESC;

-- 5. Delivery performance by seller tier
SELECT
  seller_tier,
  COUNT(*) AS num_sellers,
  ROUND(AVG(seller_delivery_performance), 2) AS avg_sla_compliance,
  ROUND(AVG(avg_delivery_days), 1) AS avg_delivery_days,
  ROUND(AVG(seller_avg_review), 2) AS avg_review_score
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance`
GROUP BY seller_tier
ORDER BY 
  CASE seller_tier
    WHEN 'Top 10 Seller' THEN 1
    WHEN 'Top 20% Revenue' THEN 2
    WHEN 'Top 50% Revenue' THEN 3
    WHEN 'Top 80% Revenue' THEN 4
    WHEN 'Bottom 20% Revenue' THEN 5
  END;

-- 6. Geographic distribution of sellers
SELECT
  seller_region,
  seller_state,
  COUNT(*) AS num_sellers,
  ROUND(SUM(seller_revenue), 2) AS total_revenue,
  ROUND(AVG(seller_revenue), 2) AS avg_revenue_per_seller,
  ROUND(AVG(seller_avg_review), 2) AS avg_review_score
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance`
GROUP BY seller_region, seller_state
ORDER BY total_revenue DESC
LIMIT 15;

-- 7. Product/category diversity impact
SELECT
  CASE
    WHEN seller_categories = 1 THEN 'Single Category'
    WHEN seller_categories BETWEEN 2 AND 3 THEN 'Low Diversity (2-3)'
    WHEN seller_categories BETWEEN 4 AND 6 THEN 'Medium Diversity (4-6)'
    ELSE 'High Diversity (7+)'
  END AS diversity_level,
  COUNT(*) AS num_sellers,
  ROUND(AVG(seller_revenue), 2) AS avg_revenue,
  ROUND(AVG(seller_orders), 1) AS avg_orders,
  ROUND(AVG(seller_avg_review), 2) AS avg_review
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance`
GROUP BY diversity_level
ORDER BY avg_revenue DESC;

-- 8. Active seller lifecycle analysis
SELECT
  CASE
    WHEN seller_active_months <= 3 THEN '0-3 months'
    WHEN seller_active_months <= 6 THEN '4-6 months'
    WHEN seller_active_months <= 12 THEN '7-12 months'
    ELSE '12+ months'
  END AS activity_period,
  COUNT(*) AS num_sellers,
  ROUND(AVG(seller_revenue), 2) AS avg_revenue,
  ROUND(AVG(revenue_per_month), 2) AS avg_revenue_per_month,
  ROUND(AVG(seller_avg_review), 2) AS avg_review
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance`
GROUP BY activity_period
ORDER BY 
  CASE activity_period
    WHEN '0-3 months' THEN 1
    WHEN '4-6 months' THEN 2
    WHEN '7-12 months' THEN 3
    WHEN '12+ months' THEN 4
  END;

-- 9. Sales concentration (HHI - Herfindahl-Hirschman Index)
WITH squared_shares AS (
  SELECT
    seller_id,
    revenue_share_pct,
    POWER(revenue_share_pct, 2) AS squared_share
  FROM `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance`
)
SELECT
  ROUND(SUM(squared_share), 2) AS hhi_index,
  CASE
    WHEN SUM(squared_share) < 100 THEN 'Highly Competitive'
    WHEN SUM(squared_share) < 1500 THEN 'Competitive'
    WHEN SUM(squared_share) < 2500 THEN 'Moderately Concentrated'
    ELSE 'Highly Concentrated'
  END AS market_concentration_level
FROM squared_shares;

-- 10. Top performers (balanced scorecard)
SELECT
  seller_id,
  seller_state,
  seller_revenue,
  seller_orders,
  seller_avg_review,
  seller_delivery_performance,
  positive_review_rate,
  revenue_rank,
  -- Balanced score (normalized)
  ROUND(
    (seller_revenue / (SELECT MAX(seller_revenue) FROM `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance`) * 0.4) +
    (seller_avg_review / 5.0 * 0.3) +
    (seller_delivery_performance / 100.0 * 0.3),
    3
  ) AS balanced_score
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_seller_performance`
WHERE seller_total_reviews >= 10  -- Minimum reviews
ORDER BY balanced_score DESC
LIMIT 20;
*/