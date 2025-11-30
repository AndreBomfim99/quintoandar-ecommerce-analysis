-- ====================================================================
-- TIME-TO-SECOND-PURCHASE ANALYSIS
-- Brazilian E-Commerce Dataset (Olist)
-- ====================================================================

-- CTE 1: Numerar compras por cliente e identificar datas
WITH customer_purchases AS (
    SELECT 
        order_id,
        customer_id,
        order_purchase_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY order_purchase_timestamp
        ) AS purchase_number
    FROM olist_orders_dataset
    WHERE order_status NOT IN ('canceled', 'unavailable') -- apenas pedidos válidos
),

-- CTE 2: Extrair primeira e segunda compra de cada cliente
purchase_dates AS (
    SELECT 
        customer_id,
        MAX(CASE WHEN purchase_number = 1 THEN order_purchase_timestamp END) AS first_purchase_date,
        MAX(CASE WHEN purchase_number = 2 THEN order_purchase_timestamp END) AS second_purchase_date
    FROM customer_purchases
    GROUP BY customer_id
),

-- CTE 3: Calcular métricas de segunda compra
second_purchase_metrics AS (
    SELECT 
        customer_id,
        first_purchase_date,
        second_purchase_date,
        CASE 
            WHEN second_purchase_date IS NOT NULL THEN 1 
            ELSE 0 
        END AS has_second_purchase,
        CASE 
            WHEN second_purchase_date IS NOT NULL 
            THEN DATE_PART('day', second_purchase_date - first_purchase_date)
            ELSE NULL 
        END AS days_to_second_purchase,
        DATE_TRUNC('month', first_purchase_date) AS cohort_month
    FROM purchase_dates
),

-- CTE 4: Criar bins de tempo
binned_data AS (
    SELECT 
        *,
        CASE 
            WHEN days_to_second_purchase IS NULL THEN 'No Second Purchase'
            WHEN days_to_second_purchase <= 30 THEN '0-30 days'
            WHEN days_to_second_purchase <= 60 THEN '31-60 days'
            WHEN days_to_second_purchase <= 90 THEN '61-90 days'
            WHEN days_to_second_purchase <= 180 THEN '91-180 days'
            ELSE '180+ days'
        END AS days_bin
    FROM second_purchase_metrics
),

-- CTE 5: Métricas gerais
overall_metrics AS (
    SELECT 
        COUNT(DISTINCT customer_id) AS total_customers,
        SUM(has_second_purchase) AS customers_with_second_purchase,
        ROUND(100.0 * SUM(has_second_purchase) / COUNT(DISTINCT customer_id), 2) AS second_purchase_rate,
        ROUND(AVG(days_to_second_purchase), 2) AS avg_days_to_second_purchase,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_second_purchase), 2) AS median_days_to_second_purchase,
        MIN(days_to_second_purchase) AS min_days,
        MAX(days_to_second_purchase) AS max_days
    FROM binned_data
),

-- CTE 6: Distribuição por bins (histogram)
time_distribution AS (
    SELECT 
        days_bin,
        COUNT(*) AS customer_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
    FROM binned_data
    GROUP BY days_bin
    ORDER BY 
        CASE days_bin
            WHEN '0-30 days' THEN 1
            WHEN '31-60 days' THEN 2
            WHEN '61-90 days' THEN 3
            WHEN '91-180 days' THEN 4
            WHEN '180+ days' THEN 5
            ELSE 6
        END
),

-- CTE 7: Taxa de segunda compra por cohort
cohort_analysis AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size,
        SUM(has_second_purchase) AS second_purchases,
        ROUND(100.0 * SUM(has_second_purchase) / COUNT(DISTINCT customer_id), 2) AS cohort_second_purchase_rate,
        ROUND(AVG(days_to_second_purchase), 2) AS avg_days_cohort
    FROM binned_data
    GROUP BY cohort_month
    ORDER BY cohort_month
),

-- CTE 8: Janelas críticas (30/60/90 dias)
critical_windows AS (
    SELECT 
        COUNT(DISTINCT customer_id) AS total_with_second_purchase,
        SUM(CASE WHEN days_to_second_purchase <= 30 THEN 1 ELSE 0 END) AS within_30_days,
        SUM(CASE WHEN days_to_second_purchase <= 60 THEN 1 ELSE 0 END) AS within_60_days,
        SUM(CASE WHEN days_to_second_purchase <= 90 THEN 1 ELSE 0 END) AS within_90_days,
        ROUND(100.0 * SUM(CASE WHEN days_to_second_purchase <= 30 THEN 1 ELSE 0 END) / 
            NULLIF(COUNT(DISTINCT customer_id), 0), 2) AS pct_within_30_days,
        ROUND(100.0 * SUM(CASE WHEN days_to_second_purchase <= 60 THEN 1 ELSE 0 END) / 
            NULLIF(COUNT(DISTINCT customer_id), 0), 2) AS pct_within_60_days,
        ROUND(100.0 * SUM(CASE WHEN days_to_second_purchase <= 90 THEN 1 ELSE 0 END) / 
            NULLIF(COUNT(DISTINCT customer_id), 0), 2) AS pct_within_90_days
    FROM binned_data
    WHERE has_second_purchase = 1
)

-- ====================================================================
-- OUTPUT FINAL: Todas as métricas consolidadas
-- ====================================================================

-- Resultado 1: MÉTRICAS GERAIS
SELECT 'OVERALL METRICS' AS section, * FROM overall_metrics
UNION ALL
-- Resultado 2: DISTRIBUIÇÃO DE TEMPO
SELECT 'TIME DISTRIBUTION' AS section, 
       days_bin::text AS metric_name,
       customer_count::numeric AS value1,
       percentage::numeric AS value2,
       NULL, NULL, NULL, NULL
FROM time_distribution
UNION ALL
-- Resultado 3: JANELAS CRÍTICAS
SELECT 'CRITICAL WINDOWS' AS section,
       'Total with 2nd purchase' AS metric_name,
       total_with_second_purchase::numeric,
       within_30_days::numeric,
       within_60_days::numeric,
       within_90_days::numeric,
       pct_within_30_days::numeric,
       pct_within_60_days::numeric
FROM critical_windows;

-- ====================================================================
-- QUERIES ADICIONAIS (descomente conforme necessário)
-- ====================================================================

-- Para ver análise detalhada por cohort:
-- SELECT * FROM cohort_analysis;

-- Para ver dados individuais de clientes:
-- SELECT * FROM binned_data ORDER BY days_to_second_purchase;

-- Para correlação com LTV (requer join com tabela de valores):
/*
SELECT 
    bd.has_second_purchase,
    bd.days_bin,
    COUNT(*) AS customers,
    ROUND(AVG(ltv.customer_lifetime_value), 2) AS avg_ltv
FROM binned_data bd
LEFT JOIN customer_ltv_table ltv ON bd.customer_id = ltv.customer_id
GROUP BY bd.has_second_purchase, bd.days_bin
ORDER BY bd.has_second_purchase DESC, bd.days_bin;
*/