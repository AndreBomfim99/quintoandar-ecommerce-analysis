-- second_purchase.sql
-- Análise de Time-to-Second-Purchase

WITH customer_orders AS (
    SELECT 
        customer_id,
        order_id,
        order_purchase_timestamp,
        -- Número da compra por cliente
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp) as purchase_number,
        -- Data da primeira compra
        FIRST_VALUE(order_purchase_timestamp) OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp) as first_purchase_date,
        -- Data da segunda compra
        LEAD(order_purchase_timestamp, 1) OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp) as second_purchase_date
    FROM olist_orders_dataset
    WHERE order_status NOT IN ('canceled')  -- Excluir pedidos cancelados
),

purchase_analysis AS (
    SELECT 
        customer_id,
        order_id,
        order_purchase_timestamp,
        purchase_number,
        first_purchase_date,
        second_purchase_date,
        -- Dias até a segunda compra
        CASE 
            WHEN purchase_number = 1 AND second_purchase_date IS NOT NULL 
            THEN DATE_PART('day', second_purchase_date - first_purchase_date)
            ELSE NULL 
        END as days_to_second_purchase,
        -- Flag se tem segunda compra
        CASE 
            WHEN MAX(purchase_number) OVER (PARTITION BY customer_id) >= 2 THEN 1
            ELSE 0 
        END as has_second_purchase,
        -- Mês da cohort (primeira compra)
        DATE_TRUNC('month', first_purchase_date) as cohort_month
    FROM customer_orders
),

cohort_analysis AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) as total_customers,
        COUNT(DISTINCT CASE WHEN has_second_purchase = 1 THEN customer_id END) as second_purchase_customers,
        ROUND(COUNT(DISTINCT CASE WHEN has_second_purchase = 1 THEN customer_id END) * 100.0 / COUNT(DISTINCT customer_id), 2) as second_purchase_rate
    FROM purchase_analysis
    WHERE purchase_number = 1  -- Apenas primeira compra para análise de cohort
    GROUP BY cohort_month
),

time_buckets AS (
    SELECT 
        CASE 
            WHEN days_to_second_purchase <= 30 THEN '0-30 dias'
            WHEN days_to_second_purchase <= 60 THEN '31-60 dias'
            WHEN days_to_second_purchase <= 90 THEN '61-90 dias'
            WHEN days_to_second_purchase <= 180 THEN '91-180 dias'
            ELSE '180+ dias'
        END as days_bin,
        COUNT(DISTINCT customer_id) as customers_count
    FROM purchase_analysis
    WHERE days_to_second_purchase IS NOT NULL
    GROUP BY 1
),

critical_windows AS (
    SELECT 
        COUNT(DISTINCT CASE WHEN days_to_second_purchase <= 30 THEN customer_id END) * 100.0 / 
        COUNT(DISTINCT customer_id) as pct_30_days,
        COUNT(DISTINCT CASE WHEN days_to_second_purchase <= 60 THEN customer_id END) * 100.0 / 
        COUNT(DISTINCT customer_id) as pct_60_days,
        COUNT(DISTINCT CASE WHEN days_to_second_purchase <= 90 THEN customer_id END) * 100.0 / 
        COUNT(DISTINCT customer_id) as pct_90_days
    FROM purchase_analysis
    WHERE has_second_purchase = 1
)

-- Métricas Finais
SELECT 
    -- Taxa de segunda compra geral
    (SELECT ROUND(COUNT(DISTINCT CASE WHEN has_second_purchase = 1 THEN customer_id END) * 100.0 / 
                  COUNT(DISTINCT customer_id), 2)
     FROM purchase_analysis WHERE purchase_number = 1) as overall_second_purchase_rate,

    -- Tempo médio até segunda compra
    (SELECT ROUND(AVG(days_to_second_purchase), 2) 
     FROM purchase_analysis WHERE days_to_second_purchase IS NOT NULL) as avg_days_to_second_purchase,

    -- Tempo mediano até segunda compra (aproximado)
    (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_second_purchase)
     FROM purchase_analysis WHERE days_to_second_purchase IS NOT NULL) as median_days_to_second_purchase,

    -- Janelas críticas
    (SELECT pct_30_days FROM critical_windows) as pct_second_purchase_30d,
    (SELECT pct_60_days FROM critical_windows) as pct_second_purchase_60d,
    (SELECT pct_90_days FROM critical_windows) as pct_second_purchase_90d;

-- Dataset principal para análise detalhada
SELECT 
    customer_id,
    order_id,
    order_purchase_timestamp,
    purchase_number,
    first_purchase_date,
    second_purchase_date,
    days_to_second_purchase,
    has_second_purchase,
    cohort_month,
    CASE 
        WHEN days_to_second_purchase <= 30 THEN '0-30 dias'
        WHEN days_to_second_purchase <= 60 THEN '31-60 dias'
        WHEN days_to_second_purchase <= 90 THEN '61-90 dias'
        WHEN days_to_second_purchase <= 180 THEN '91-180 dias'
        ELSE '180+ dias'
    END as days_bin
FROM purchase_analysis
WHERE purchase_number = 1;  -- Apenas o registro da primeira compra por cliente

-- Distribuição de tempo (histogram)
SELECT 
    days_bin,
    customers_count,
    ROUND(customers_count * 100.0 / SUM(customers_count) OVER (), 2) as percentage
FROM time_buckets
ORDER BY 
    CASE days_bin
        WHEN '0-30 dias' THEN 1
        WHEN '31-60 dias' THEN 2
        WHEN '61-90 dias' THEN 3
        WHEN '91-180 dias' THEN 4
        ELSE 5
    END;

-- Taxa de segunda compra por cohort
SELECT 
    cohort_month,
    total_customers,
    second_purchase_customers,
    second_purchase_rate
FROM cohort_analysis
ORDER BY cohort_month;