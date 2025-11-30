-- =============================================
-- second_purchase.sql
-- Análise completa: Time to Second Purchase
-- Dataset: Brazilian E-Commerce by Olist (BigQuery)
-- =============================================

WITH orders_clean AS (
  -- Pegamos apenas os pedidos entregues (para garantir que a compra foi concluída)
  SELECT 
    customer_id,
    order_id,
    order_purchase_timestamp,
    DATE(order_purchase_timestamp) AS purchase_date
  FROM `olist-public-dataset.olist_orders_dataset.olist_orders_dataset`
  WHERE order_status = 'delivered'
),

-- Numeramos as compras de cada cliente
ranked_purchases AS (
  SELECT 
    customer_id,
    order_id,
    order_purchase_timestamp,
    purchase_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp) AS purchase_number,
    MIN(order_purchase_timestamp) OVER (PARTITION BY customer_id) AS first_purchase_timestamp,
    MAX(order_purchase_timestamp) OVER (PARTITION BY customer_id) AS last_purchase_timestamp
  FROM orders_clean
),

-- Identificamos primeira e segunda compra
second_purchase_details AS (
  SELECT
    customer_id,
    MIN(CASE WHEN purchase_number = 1 THEN order_purchase_timestamp END) AS first_purchase_date,
    MIN(CASE WHEN purchase_number = 2 THEN order_purchase_timestamp END) AS second_purchase_date,
    MIN(CASE WHEN purchase_number = 1 THEN purchase_date END) AS first_purchase_cohort_date
  FROM ranked_purchases
  GROUP BY customer_id
),

-- Calculamos as métricas por cliente
customer_metrics AS (
  SELECT
    customer_id,
    first_purchase_date,
    first_purchase_cohort_date,
    EXTRACT(YEAR FROM first_purchase_date) * 100 + EXTRACT(MONTH FROM first_purchase_date) AS cohort_month,
    second_purchase_date,
    DATE_DIFF(second_purchase_date, first_purchase_date, DAY) AS days_to_second_purchase,
    IF(second_purchase_date IS NOT NULL, 1, 0) AS has_second_purchase
  FROM second_purchase_details
),

-- Binning dos dias
customer_metrics_binned AS (
  SELECT *,
    CASE 
      WHEN days_to_second_purchase <= 30   THEN '0-30 dias'
      WHEN days_to_second_purchase <= 60   THEN '31-60 dias'
      WHEN days_to_second_purchase <= 90   THEN '61-90 dias'
      WHEN days_to_second_purchase <= 180  THEN '91-180 dias'
      ELSE '180+ dias'
    END AS days_bin
  FROM customer_metrics
  WHERE first_purchase_date >= '2016-10-01' -- início real dos dados relevantes
)

-- =============================================
-- MÉTRICAS FINAIS
-- =============================================

-- 1. Métricas gerais
SELECT
  ROUND(100.0 * AVG(has_second_purchase), 2) AS second_purchase_rate_percent,
  ROUND(AVG(IF(has_second_purchase = 1, days_to_second_purchase, NULL)), 1) AS avg_days_to_second_purchase,
  ROUND(PERCENTILE_CONT(IF(has_second_purchase = 1, days_to_second_purchase, NULL), 0.5) 
        OVER(), 1) AS median_days_to_second_purchase,
  COUNTIF(has_second_purchase = 1) AS customers_with_2nd_purchase,
  COUNT(*) AS total_customers
FROM customer_metrics_binned
LIMIT 1;

-- 2. Distribuição (histograma) do tempo até a segunda compra
SELECT
  days_bin,
  COUNTIF(has_second_purchase = 1) AS customers_with_2nd_purchase,
  ROUND(100.0 * COUNTIF(has_second_purchase = 1) / SUM(COUNTIF(has_second_purchase = 1)) OVER(), 2) AS pct_distribution
FROM customer_metrics_binned
WHERE has_second_purchase = 1
GROUP BY days_bin
ORDER BY 
  CASE days_bin
    WHEN '0-30 dias' THEN 1
    WHEN '31-60 dias' THEN 2
    WHEN '61-90 dias' THEN 3
    WHEN '91-180 dias' THEN 4
    ELSE 5
  END;

-- 3. Taxa de segunda compra por cohort (mês da primeira compra)
SELECT
  cohort_month,
  FORMAT_DATE('%Y-%m', DATE_TRUNC(first_purchase_cohort_date, MONTH)) AS cohort_month_label,
  COUNT(*) AS cohort_size,
  COUNTIF(has_second_purchase = 1) AS repeaters,
  ROUND(100.0 * COUNTIF(has_second_purchase = 1) / COUNT(*), 2) AS second_purchase_rate_percent
FROM customer_metrics_binned
GROUP BY cohort_month, first_purchase_cohort_date
ORDER BY cohort_month;

-- 4. Janela crítica: % que voltam nos primeiros 30/60/90 dias
SELECT
  ROUND(100.0 * COUNTIF(has_second_purchase = 1 AND days_to_second_purchase <= 30) / COUNTIF(has_second_purchase = 1), 2) AS pct_within_30_days,
  ROUND(100.0 * COUNTIF(has_second_purchase = 1 AND days_to_second_purchase <= 60) / COUNTIF(has_second_purchase = 1), 2) AS pct_within_60_days,
  ROUND(100.0 * COUNTIF(has_second_purchase = 1 AND days_to_second_purchase <= 90) / COUNTIF(has_second_purchase = 1), 2) AS pct_within_90_days
FROM customer_metrics_binned
LIMIT 1;

-- 5. (Bônus) Correlação entre tempo até 2ª compra e LTV futuro
-- Primeiro calculamos o LTV por cliente (valor total pago em todos os pedidos)
WITH customer_ltv AS (
  SELECT
    o.customer_id,
    SUM(p.payment_value) AS ltv
  FROM `olist-public-dataset.olist_orders_dataset.olist_orders_dataset` o
  JOIN `olist-public-dataset.olist_order_payments_dataset.olist_order_payments_dataset` p
    ON o.order_id = p.order_id
  WHERE o.order_status = 'delivered'
  GROUP BY customer_id
)

SELECT
  CORR(days_to_second_purchase, ltv) AS corr_days_to_second_vs_ltv
FROM customer_metrics_binned c
JOIN customer_ltv l USING (customer_id)
WHERE c.has_second_purchase = 1;