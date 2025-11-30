-- =========================================================
-- MART: UNIT ECONOMICS
-- =========================================================
-- Description: Unit economics analysis by segment (CAC, LTV, margins, payback)
-- Based on: Analysis #8 (Unit Economics Analysis)
-- Source: mart_customer_ltv, mart_customer_rfm, stg_order_items, stg_payments
-- Destination: olist_marts.mart_unit_economics
-- Note: CAC is estimated using benchmark proxy (R$50)
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis`.`olist_marts.mart_unit_economics` AS
WITH
  customer_revenue_metrics AS (
    SELECT
      ltv.customer_id,
      ltv.customer_state,
      ltv.customer_region,
      rfm.rfm_segment,
      ltv.total_revenue AS revenue_per_customer,
      ltv.total_orders AS orders_per_customer,
      ltv.avg_order_value,
      ltv.customer_lifespan_days,
      ltv.orders_per_month,
      ROUND(ltv.customer_lifespan_days / 30.0, 2) AS customer_lifespan_months
    FROM
      `quintoandar-ecommerce-analysis`.olist_marts.mart_customer_ltv AS ltv
      INNER JOIN
      `quintoandar-ecommerce-analysis`.olist_marts.mart_customer_rfm AS rfm
      ON ltv.customer_id = rfm.customer_id
  ),
  customer_item_economics AS (
    SELECT
      o.customer_id,
      ROUND(SUM(oi.price), 2) AS gross_revenue,
      ROUND(SUM(oi.freight_value), 2) AS total_freight,
      ROUND(SUM(oi.price) - SUM(oi.freight_value), 2) AS net_revenue,
      ROUND(AVG((oi.price - oi.freight_value) / NULLIF(oi.price, 0)) * 100, 2) AS margin_proxy_pct
    FROM
      `quintoandar-ecommerce-analysis`.olist_staging.stg_orders AS o
      INNER JOIN
      `quintoandar-ecommerce-analysis`.olist_staging.stg_order_items AS oi
      ON o.order_id = oi.order_id
    WHERE
      o.is_delivered OR o.is_completed
    GROUP BY o.customer_id
  ),
  customer_unit_economics AS (
    SELECT
      crm.*,
      COALESCE(cie.gross_revenue, 0) AS gross_revenue,
      COALESCE(cie.total_freight, 0) AS total_freight,
      COALESCE(cie.net_revenue, 0) AS net_revenue,
      COALESCE(cie.margin_proxy_pct, 0) AS margin_proxy_pct,
      50.0 AS cac_proxy,
      crm.revenue_per_customer AS ltv_calculated,
      ROUND(crm.revenue_per_customer / 50.0, 2) AS ltv_cac_ratio,
      ROUND(
        CASE
          WHEN crm.customer_lifespan_months > 0 THEN crm.revenue_per_customer / crm.customer_lifespan_months
          ELSE crm.revenue_per_customer
        END, 2) AS monthly_revenue_per_customer,
      ROUND(
        CASE
          WHEN crm.customer_lifespan_months > 0 THEN 50.0 / (crm.revenue_per_customer / crm.customer_lifespan_months)
          ELSE NULL
        END, 2) AS payback_months,
      ROUND(COALESCE(cie.net_revenue, 0) - 50.0, 2) AS contribution_margin
    FROM
      customer_revenue_metrics AS crm
      LEFT JOIN
      customer_item_economics AS cie
      ON crm.customer_id = cie.customer_id
  ),
  unit_economics_by_rfm AS (
    SELECT
      'RFM Segment' AS segmentation_type,
      rfm_segment AS segment_name,
      CAST(NULL AS STRING) AS segment_region,
      COUNT(*) AS num_customers,
      ROUND(AVG(revenue_per_customer), 2) AS avg_ltv,
      ROUND(SUM(revenue_per_customer), 2) AS total_revenue,
      ROUND(AVG(orders_per_customer), 2) AS avg_orders,
      ROUND(AVG(avg_order_value), 2) AS avg_aov,
      ROUND(AVG(gross_revenue), 2) AS avg_gross_revenue,
      ROUND(AVG(total_freight), 2) AS avg_total_freight,
      ROUND(AVG(net_revenue), 2) AS avg_net_revenue,
      ROUND(AVG(margin_proxy_pct), 2) AS avg_margin_pct,
      50.0 AS avg_cac_proxy,
      ROUND(AVG(ltv_cac_ratio), 2) AS avg_ltv_cac_ratio,
      ROUND(AVG(payback_months), 2) AS avg_payback_months,
      ROUND(AVG(contribution_margin), 2) AS avg_contribution_margin,
      ROUND(AVG(customer_lifespan_months), 2) AS avg_customer_lifespan_months,
      ROUND(AVG(monthly_revenue_per_customer), 2) AS avg_monthly_revenue
    FROM
      customer_unit_economics
    GROUP BY rfm_segment
  ),
  unit_economics_by_state AS (
    SELECT
      'State' AS segmentation_type,
      customer_state AS segment_name,
      customer_region AS segment_region,
      COUNT(*) AS num_customers,
      ROUND(AVG(revenue_per_customer), 2) AS avg_ltv,
      ROUND(SUM(revenue_per_customer), 2) AS total_revenue,
      ROUND(AVG(orders_per_customer), 2) AS avg_orders,
      ROUND(AVG(avg_order_value), 2) AS avg_aov,
      ROUND(AVG(gross_revenue), 2) AS avg_gross_revenue,
      ROUND(AVG(total_freight), 2) AS avg_total_freight,
      ROUND(AVG(net_revenue), 2) AS avg_net_revenue,
      ROUND(AVG(margin_proxy_pct), 2) AS avg_margin_pct,
      50.0 AS avg_cac_proxy,
      ROUND(AVG(ltv_cac_ratio), 2) AS avg_ltv_cac_ratio,
      ROUND(AVG(payback_months), 2) AS avg_payback_months,
      ROUND(AVG(contribution_margin), 2) AS avg_contribution_margin,
      ROUND(AVG(customer_lifespan_months), 2) AS avg_customer_lifespan_months,
      ROUND(AVG(monthly_revenue_per_customer), 2) AS avg_monthly_revenue
    FROM
      customer_unit_economics
    GROUP BY customer_state, customer_region
  ),
  unit_economics_by_region AS (
    SELECT
      'Region' AS segmentation_type,
      customer_region AS segment_name,
      CAST(NULL AS STRING) AS segment_region,
      COUNT(*) AS num_customers,
      ROUND(AVG(revenue_per_customer), 2) AS avg_ltv,
      ROUND(SUM(revenue_per_customer), 2) AS total_revenue,
      ROUND(AVG(orders_per_customer), 2) AS avg_orders,
      ROUND(AVG(avg_order_value), 2) AS avg_aov,
      ROUND(AVG(gross_revenue), 2) AS avg_gross_revenue,
      ROUND(AVG(total_freight), 2) AS avg_total_freight,
      ROUND(AVG(net_revenue), 2) AS avg_net_revenue,
      ROUND(AVG(margin_proxy_pct), 2) AS avg_margin_pct,
      50.0 AS avg_cac_proxy,
      ROUND(AVG(ltv_cac_ratio), 2) AS avg_ltv_cac_ratio,
      ROUND(AVG(payback_months), 2) AS avg_payback_months,
      ROUND(AVG(contribution_margin), 2) AS avg_contribution_margin,
      ROUND(AVG(customer_lifespan_months), 2) AS avg_customer_lifespan_months,
      ROUND(AVG(monthly_revenue_per_customer), 2) AS avg_monthly_revenue
    FROM
      customer_unit_economics
    GROUP BY customer_region
  ),
  unit_economics_by_cohort AS (
    SELECT
      'Cohort' AS segmentation_type,
      FORMAT_TIMESTAMP('%Y-Q%Q', ltv.first_purchase_date) AS segment_name,
      CAST(NULL AS STRING) AS segment_region,
      COUNT(*) AS num_customers,
      ROUND(AVG(cue.revenue_per_customer), 2) AS avg_ltv,
      ROUND(SUM(cue.revenue_per_customer), 2) AS total_revenue,
      ROUND(AVG(cue.orders_per_customer), 2) AS avg_orders,
      ROUND(AVG(cue.avg_order_value), 2) AS avg_aov,
      ROUND(AVG(cue.gross_revenue), 2) AS avg_gross_revenue,
      ROUND(AVG(cue.total_freight), 2) AS avg_total_freight,
      ROUND(AVG(cue.net_revenue), 2) AS avg_net_revenue,
      ROUND(AVG(cue.margin_proxy_pct), 2) AS avg_margin_pct,
      50.0 AS avg_cac_proxy,
      ROUND(AVG(cue.ltv_cac_ratio), 2) AS avg_ltv_cac_ratio,
      ROUND(AVG(cue.payback_months), 2) AS avg_payback_months,
      ROUND(AVG(cue.contribution_margin), 2) AS avg_contribution_margin,
      ROUND(AVG(cue.customer_lifespan_months), 2) AS avg_customer_lifespan_months,
      ROUND(AVG(cue.monthly_revenue_per_customer), 2) AS avg_monthly_revenue
    FROM
      customer_unit_economics AS cue
      INNER JOIN
      `quintoandar-ecommerce-analysis`.olist_marts.mart_customer_ltv AS ltv
      ON cue.customer_id = ltv.customer_id
    GROUP BY FORMAT_TIMESTAMP('%Y-Q%Q', ltv.first_purchase_date)
  ),
  all_segmentations AS (
    SELECT
      *
    FROM
      unit_economics_by_rfm
    UNION ALL
    SELECT
      *
    FROM
      unit_economics_by_state
    UNION ALL
    SELECT
      *
    FROM
      unit_economics_by_region
    UNION ALL
    SELECT
      *
    FROM
      unit_economics_by_cohort
  )
SELECT
  *
FROM
  all_segmentations
ORDER BY segmentation_type, total_revenue DESC;



/*
-- VALIDATION QUERIES
-- 1. Unit economics by RFM segment
SELECT
  segment_name,
  num_customers,
  avg_ltv,
  avg_cac_proxy,
  avg_ltv_cac_ratio,
  avg_payback_months,
  avg_contribution_margin
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_unit_economics`
WHERE segmentation_type = 'RFM Segment'
ORDER BY avg_ltv DESC;

-- 2. Unit economics by state (top 10)
SELECT
  segment_name AS state,
  segment_region AS region,
  num_customers,
  avg_ltv,
  avg_ltv_cac_ratio,
  avg_margin_pct,
  avg_contribution_margin
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_unit_economics`
WHERE segmentation_type = 'State'
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Unit economics by region
SELECT
  segment_name AS region,
  num_customers,
  total_revenue,
  avg_ltv,
  avg_aov,
  avg_ltv_cac_ratio,
  avg_payback_months
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_unit_economics`
WHERE segmentation_type = 'Region'
ORDER BY total_revenue DESC;

-- 4. Unit economics by cohort
SELECT
  segment_name AS cohort,
  num_customers,
  avg_ltv,
  avg_ltv_cac_ratio,
  avg_customer_lifespan_months,
  avg_monthly_revenue
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_unit_economics`
WHERE segmentation_type = 'Cohort'
ORDER BY segment_name;

-- 5. Profitability analysis (segments with best LTV/CAC)
SELECT
  segmentation_type,
  segment_name,
  num_customers,
  avg_ltv,
  avg_ltv_cac_ratio,
  avg_contribution_margin,
  CASE
    WHEN avg_ltv_cac_ratio >= 3 THEN 'Excellent'
    WHEN avg_ltv_cac_ratio >= 2 THEN 'Good'
    WHEN avg_ltv_cac_ratio >= 1 THEN 'Break Even'
    ELSE 'Loss'
  END AS profitability_category
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_unit_economics`
WHERE segmentation_type IN ('RFM Segment', 'Region')
ORDER BY avg_ltv_cac_ratio DESC;

-- 6. Margin analysis by segment
SELECT
  segmentation_type,
  segment_name,
  avg_gross_revenue,
  avg_total_freight,
  avg_net_revenue,
  avg_margin_pct,
  avg_contribution_margin
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_unit_economics`
WHERE segmentation_type = 'RFM Segment'
ORDER BY avg_margin_pct DESC;

-- 7. Payback period analysis
SELECT
  segmentation_type,
  segment_name,
  avg_ltv,
  avg_cac_proxy,
  avg_payback_months,
  avg_monthly_revenue,
  CASE
    WHEN avg_payback_months <= 3 THEN 'Fast (0-3 months)'
    WHEN avg_payback_months <= 6 THEN 'Medium (3-6 months)'
    WHEN avg_payback_months <= 12 THEN 'Slow (6-12 months)'
    ELSE 'Very Slow (12+ months)'
  END AS payback_category
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_unit_economics`
WHERE segmentation_type = 'RFM Segment'
  AND avg_payback_months IS NOT NULL
ORDER BY avg_payback_months;

-- 8. Revenue concentration by segment type
SELECT
  segmentation_type,
  COUNT(DISTINCT segment_name) AS num_segments,
  ROUND(SUM(total_revenue), 2) AS total_revenue,
  ROUND(AVG(avg_ltv), 2) AS overall_avg_ltv,
  ROUND(AVG(avg_ltv_cac_ratio), 2) AS overall_avg_ltv_cac_ratio
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_unit_economics`
GROUP BY segmentation_type
ORDER BY total_revenue DESC;

-- 9. Best performing segments overall
SELECT
  segmentation_type,
  segment_name,
  num_customers,
  total_revenue,
  avg_ltv_cac_ratio,
  avg_contribution_margin,
  avg_margin_pct
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_unit_economics`
WHERE segmentation_type IN ('RFM Segment', 'State')
  AND num_customers >= 100  -- Minimum sample size
ORDER BY avg_ltv_cac_ratio DESC
LIMIT 15;
*/