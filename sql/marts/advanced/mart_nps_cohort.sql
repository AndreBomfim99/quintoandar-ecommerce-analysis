-- =========================================================
-- MART: NPS COHORT ANALYSIS  
-- =========================================================
-- Description: Net Promoter Score impact on retention and LTV
-- Sources: stg_reviews, stg_orders, stg_payments, mart_customer_base
-- Destination: olist_marts.mart_nps_cohort
-- Granularity: 1 row per customer with NPS cohort analysis
-- =========================================================

CREATE OR REPLACE TABLE `quintoandar-ecommerce-analysis`.`olist_marts.mart_nps_cohort` AS
WITH
  review_base AS (
    SELECT
      r.order_id,
      r.review_id,
      r.review_score,
      r.review_creation_date,
      r.has_comment,
      CASE
        WHEN r.review_score IN (4, 5) THEN 'Promoter'
        WHEN r.review_score = 3 THEN 'Passive'
        WHEN r.review_score IN (1, 2) THEN 'Detractor'
        ELSE 'Unknown'
      END AS nps_category,
      CASE
        WHEN r.review_score IN (1, 2) THEN 1
        ELSE 0
      END AS is_negative_review,
      CASE
        WHEN r.review_score = 5 THEN 1
        ELSE 0
      END AS is_positive_review
    FROM
      `quintoandar-ecommerce-analysis`.olist_staging.stg_reviews AS r
  ),
  customer_review_metrics AS (
    SELECT
      o.customer_id,
      AVG(r.review_score) AS avg_review_score,
      COUNT(r.review_id) AS total_reviews,
      COUNT(DISTINCT r.nps_category) AS unique_nps_categories,
      SUM(
        CASE
          WHEN r.nps_category = 'Promoter' THEN 1
          ELSE 0
        END) AS promoter_reviews,
      SUM(
        CASE
          WHEN r.nps_category = 'Passive' THEN 1
          ELSE 0
        END) AS passive_reviews,
      SUM(
        CASE
          WHEN r.nps_category = 'Detractor' THEN 1
          ELSE 0
        END) AS detractor_reviews,
      MAX(r.is_negative_review) AS has_negative_review,
      MAX(r.is_positive_review) AS has_positive_review,
      CASE
        WHEN SUM(
          CASE
            WHEN r.nps_category = 'Promoter' THEN 1
            ELSE 0
          END) > SUM(
          CASE
            WHEN r.nps_category = 'Detractor' THEN 1
            ELSE 0
          END) AND SUM(
          CASE
            WHEN r.nps_category = 'Promoter' THEN 1
            ELSE 0
          END) > SUM(
          CASE
            WHEN r.nps_category = 'Passive' THEN 1
            ELSE 0
          END) THEN 'Promoter'
        WHEN SUM(
          CASE
            WHEN r.nps_category = 'Detractor' THEN 1
            ELSE 0
          END) > SUM(
          CASE
            WHEN r.nps_category = 'Promoter' THEN 1
            ELSE 0
          END) AND SUM(
          CASE
            WHEN r.nps_category = 'Detractor' THEN 1
            ELSE 0
          END) > SUM(
          CASE
            WHEN r.nps_category = 'Passive' THEN 1
            ELSE 0
          END) THEN 'Detractor'
        ELSE 'Passive'
      END AS primary_nps_category
    FROM
      `quintoandar-ecommerce-analysis`.olist_staging.stg_orders AS o
      INNER JOIN
      review_base AS r
      ON o.order_id = r.order_id
    WHERE
      o.is_completed = TRUE
    GROUP BY o.customer_id
  ),
  first_order_nps AS (
    SELECT
      o.customer_id,
      r.review_score AS first_order_review_score,
      r.nps_category AS first_order_nps_category,
      r.is_negative_review AS first_order_negative
    FROM
      `quintoandar-ecommerce-analysis`.olist_staging.stg_orders AS o
      INNER JOIN
      review_base AS r
      ON o.order_id = r.order_id
    WHERE
      o.is_completed = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY o.customer_id
      ORDER BY o.order_purchase_timestamp) = 1
  ),
  nps_trend_analysis AS (
    SELECT
      o.customer_id,
      CASE
        WHEN COUNT(r.review_id) >= 2 THEN
        CASE
          WHEN MAX(r.review_score) - MIN(r.review_score) > 1 THEN 'Improving'
          WHEN MIN(r.review_score) - MAX(r.review_score) > 1 THEN 'Declining'
          ELSE 'Stable'
        END
        ELSE 'Single Review'
      END AS nps_trend,
      CASE
        WHEN COUNT(r.review_id) >= 2 THEN MAX(r.review_score) - MIN(r.review_score)
        ELSE 0
      END AS score_volatility
    FROM
      `quintoandar-ecommerce-analysis`.olist_staging.stg_orders AS o
      INNER JOIN
      review_base AS r
      ON o.order_id = r.order_id
    WHERE
      o.is_completed = TRUE
    GROUP BY o.customer_id
  ),
  customer_behavior_metrics AS (
    SELECT
      crm.customer_id,
      crm.avg_review_score,
      crm.total_reviews,
      crm.primary_nps_category,
      crm.has_negative_review,
      crm.has_positive_review,
      crm.promoter_reviews,
      crm.passive_reviews,
      crm.detractor_reviews,
      fon.first_order_review_score,
      fon.first_order_nps_category,
      fon.first_order_negative,
      nta.nps_trend,
      nta.score_volatility,
      cb.total_orders,
      cb.total_revenue,
      cb.avg_order_value,
      cb.is_repeat_customer,
      cb.customer_lifespan_days,
      cb.recency_days,
      (crm.promoter_reviews - crm.detractor_reviews) * 100.0 / NULLIF(crm.total_reviews, 0) AS customer_nps_score
    FROM
      customer_review_metrics AS crm
      LEFT JOIN
      first_order_nps AS fon
      ON crm.customer_id = fon.customer_id
      LEFT JOIN
      nps_trend_analysis AS nta
      ON crm.customer_id = nta.customer_id
      LEFT JOIN
      `quintoandar-ecommerce-analysis`.olist_marts.mart_customer_base AS cb
      ON crm.customer_id = cb.customer_id
  ),
  recovery_analysis AS (
    SELECT
      cr.customer_id,
      CASE
        WHEN cr.has_negative_review = 1 AND cr.primary_nps_category = 'Promoter' THEN 1
        ELSE 0
      END AS is_recovered_detractor,
      CASE
        WHEN cr.first_order_nps_category = 'Detractor' AND cr.primary_nps_category = 'Promoter' THEN 'Detractor_to_Promoter'
        WHEN cr.first_order_nps_category = 'Detractor' AND cr.primary_nps_category = 'Passive' THEN 'Detractor_to_Passive'
        WHEN cr.first_order_nps_category = 'Promoter' AND cr.primary_nps_category = 'Detractor' THEN 'Promoter_to_Detractor'
        ELSE 'Stable'
      END AS nps_journey
    FROM
      customer_behavior_metrics AS cr
  ),
  final_nps_cohort AS (
    SELECT
      cbm.*,
      ra.is_recovered_detractor,
      ra.nps_journey,
      CASE
        WHEN cbm.primary_nps_category = 'Promoter' THEN cbm.total_revenue * 0.3
        WHEN cbm.primary_nps_category = 'Passive' THEN cbm.total_revenue * 0.1
        ELSE cbm.total_revenue * -0.2
      END AS estimated_nps_impact_value,
      CASE
        WHEN cbm.primary_nps_category = 'Promoter' THEN 0.7
        WHEN cbm.primary_nps_category = 'Passive' THEN 0.4
        WHEN cbm.primary_nps_category = 'Detractor' THEN 0.1
        ELSE 0.3
      END AS estimated_retention_probability,
      CASE
        WHEN cbm.primary_nps_category = 'Promoter' AND cbm.total_revenue > 500 THEN 'High_Value_Promoter'
        WHEN cbm.primary_nps_category = 'Promoter' THEN 'Standard_Promoter'
        WHEN cbm.primary_nps_category = 'Detractor' AND cbm.total_revenue > 500 THEN 'At_Risk_High_Value'
        WHEN cbm.primary_nps_category = 'Detractor' THEN 'Standard_Detractor'
        ELSE 'Neutral_Customer'
      END AS nps_value_segment
    FROM
      customer_behavior_metrics AS cbm
      LEFT JOIN
      recovery_analysis AS ra
      ON cbm.customer_id = ra.customer_id
  )
SELECT
  *
FROM
  final_nps_cohort
ORDER BY total_revenue DESC, avg_review_score DESC;




/*
-- VALIDATION QUERIES  
-- 1. overall NPS score calculation
SELECT
  COUNT(*) AS total_customers,
  SUM(CASE WHEN primary_nps_category = 'Promoter' THEN 1 ELSE 0 END) AS promoters,
  SUM(CASE WHEN primary_nps_category = 'Passive' THEN 1 ELSE 0 END) AS passives,
  SUM(CASE WHEN primary_nps_category = 'Detractor' THEN 1 ELSE 0 END) AS detractors,
  ROUND(
    (SUM(CASE WHEN primary_nps_category = 'Promoter' THEN 1 ELSE 0 END) - 
     SUM(CASE WHEN primary_nps_category = 'Detractor' THEN 1 ELSE 0 END)) * 100.0 / 
    COUNT(*), 2
  ) AS net_promoter_score
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_nps_cohort`;

-- 2. LTV by NPS Category
SELECT
  primary_nps_category,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(AVG(avg_order_value), 2) AS avg_aov,
  ROUND(AVG(total_orders), 2) AS avg_orders,
  ROUND(SUM(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS retention_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_nps_cohort`
GROUP BY primary_nps_category
ORDER BY avg_ltv DESC;

-- 3. first order NPS impact analysis
SELECT
  first_order_nps_category,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(SUM(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS retention_rate,
  ROUND(AVG(customer_lifespan_days), 1) AS avg_lifespan_days
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_nps_cohort`
GROUP BY first_order_nps_category
ORDER BY retention_rate DESC;

-- 4. recovery analysis detractors who became ppromoters
SELECT
  nps_journey,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(AVG(avg_review_score), 2) AS avg_final_score,
  ROUND(AVG(total_orders), 2) AS avg_orders
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_nps_cohort`
WHERE nps_journey != 'Stable'
GROUP BY nps_journey
ORDER BY customer_count DESC;

-- 5. NPS trend analysis
SELECT
  nps_trend,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(AVG(avg_review_score), 2) AS avg_score,
  ROUND(SUM(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS retention_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_nps_cohort`
GROUP BY nps_trend
ORDER BY customer_count DESC;

-- 6. economic impact of NPS improvement
SELECT
  primary_nps_category,
  COUNT(*) AS customers,
  ROUND(SUM(total_revenue), 2) AS total_revenue,
  ROUND(SUM(estimated_nps_impact_value), 2) AS estimated_impact,
  ROUND(AVG(estimated_retention_probability), 3) AS avg_retention_prob
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_nps_cohort`
GROUP BY primary_nps_category
ORDER BY estimated_impact DESC;

-- 7. high value customer analysis by NPS
SELECT
  nps_value_segment,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_revenue), 2) AS avg_ltv,
  ROUND(AVG(avg_review_score), 2) AS avg_score,
  ROUND(SUM(CASE WHEN is_recovered_detractor THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS recovery_rate
FROM `quintoandar-ecommerce-analysis.olist_marts.mart_nps_cohort`
GROUP BY nps_value_segment
ORDER BY avg_ltv DESC;
*/