-- second_purchase.sql

WITH RankedOrders AS (
    -- 1. Classifica as compras de cada cliente em ordem cronológica
    SELECT
        customer_id,
        order_purchase_timestamp,
        -- Coluna: purchase_number
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_purchase_timestamp
        ) AS purchase_number
    FROM
        olist_orders_dataset
),

CustomerPurchases AS (
    -- 2. Identifica as datas da primeira e segunda compra para cada cliente
    SELECT
        customer_id,
        -- Coluna: first_purchase_date
        MIN(CASE WHEN purchase_number = 1 THEN order_purchase_timestamp END) AS first_purchase_date,
        -- Coluna: second_purchase_date
        MIN(CASE WHEN purchase_number = 2 THEN order_purchase_timestamp END) AS second_purchase_date
    FROM
        RankedOrders
    GROUP BY
        customer_id
),

TimeMetrics AS (
    -- 3. Calcula as métricas de tempo e bins de tempo
    SELECT
        customer_id,
        first_purchase_date,
        second_purchase_date,
        
        -- Coluna: has_second_purchase
        CASE WHEN second_purchase_date IS NOT NULL THEN 1 ELSE 0 END AS has_second_purchase,
        
        -- Coluna: days_to_second_purchase (assumindo PostgreSQL para a função DATE_PART)
        -- Para MySQL: DATEDIFF(second_purchase_date, first_purchase_date)
        -- Para PostgreSQL:
        DATE_PART('day', second_purchase_date::timestamp - first_purchase_date::timestamp) AS days_to_second_purchase,

        -- Coluna: cohort_month (Mês da primeira compra)
        DATE_TRUNC('month', first_purchase_date)::date AS cohort_month

    FROM
        CustomerPurchases
),

FinalAnalysis AS (
    -- 4. Cria o binning e a taxa de segunda compra
    SELECT
        *,
        
        -- Coluna: days_bin
        CASE
            WHEN days_to_second_purchase IS NULL THEN 'N/A'
            WHEN days_to_second_purchase BETWEEN 0 AND 30 THEN '0-30 dias'
            WHEN days_to_second_purchase BETWEEN 31 AND 60 THEN '31-60 dias'
            WHEN days_to_second_purchase BETWEEN 61 AND 90 THEN '61-90 dias'
            WHEN days_to_second_purchase BETWEEN 91 AND 180 THEN '91-180 dias'
            ELSE '180+ dias'
        END AS days_bin,
        
        -- Coluna: second_purchase_rate (Calculada no nível da análise final)
        -- Esta é uma métrica geral que é mais fácil de calcular no último CTE ou como métrica final.
        -- Vou deixar o cálculo da taxa para as MÉTRICAS FINAIS abaixo, pois é uma agregação.
        NULL AS second_purchase_rate -- Coluna auxiliar, mas será preenchida na etapa de métricas
        
    FROM
        TimeMetrics
)

-- ************************************************************
-- MÉTRICAS FINAIS
-- ************************************************************

-- 5. CÁLCULO DAS MÉTRICAS GERAIS E AGRUPADAS

-- A. Taxa de Segunda Compra Geral, Tempo Médio/Mediano, Janela Crítica
SELECT
    'Métricas Gerais' AS metric_group,
    -- Coluna: second_purchase_rate (Taxa de segunda compra geral)
    SUM(has_second_purchase)::float / COUNT(customer_id) AS overall_second_purchase_rate,
    
    -- Tempo Médio até a segunda compra
    AVG(days_to_second_purchase) AS avg_days_to_second_purchase,
    
    -- Tempo Mediano até a segunda compra (Usando PERCENTILE_CONT - PostgreSQL)
    -- Para MySQL/outros, você precisaria de uma função de janela ou uma abordagem diferente
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_second_purchase) AS median_days_to_second_purchase,
    
    -- Janela Crítica: % de clientes que compram nos primeiros 30, 60 e 90 dias
    SUM(CASE WHEN days_to_second_purchase BETWEEN 0 AND 30 THEN 1 ELSE 0 END)::float / SUM(has_second_purchase) AS pct_30_days,
    SUM(CASE WHEN days_to_second_purchase BETWEEN 0 AND 60 THEN 1 ELSE 0 END)::float / SUM(has_second_purchase) AS pct_60_days,
    SUM(CASE WHEN days_to_second_purchase BETWEEN 0 AND 90 THEN 1 ELSE 0 END)::float / SUM(has_second_purchase) AS pct_90_days
FROM
    FinalAnalysis
WHERE
    first_purchase_date IS NOT NULL

UNION ALL

-- B. Distribuição de Tempo (Histograma)
SELECT
    'Distribuição de Tempo' AS metric_group,
    NULL AS overall_second_purchase_rate,
    NULL AS avg_days_to_second_purchase,
    NULL AS median_days_to_second_purchase,
    NULL AS pct_30_days,
    NULL AS pct_60_days,
    NULL AS pct_90_days,
    
    days_bin,
    COUNT(customer_id) AS customer_count,
    COUNT(customer_id) * 100.0 / SUM(COUNT(customer_id)) OVER () AS pct_of_second_purchases
FROM
    FinalAnalysis
WHERE
    days_to_second_purchase IS NOT NULL
GROUP BY
    days_bin
ORDER BY
    CASE 
        WHEN days_bin = '0-30 dias' THEN 1
        WHEN days_bin = '31-60 dias' THEN 2
        WHEN days_bin = '61-90 dias' THEN 3
        WHEN days_bin = '91-180 dias' THEN 4
        WHEN days_bin = '180+ dias' THEN 5
        ELSE 99 
    END

UNION ALL

-- C. Taxa de Segunda Compra por Cohort
SELECT
    'Taxa por Cohort' AS metric_group,
    NULL AS overall_second_purchase_rate,
    NULL AS avg_days_to_second_purchase,
    NULL AS median_days_to_second_purchase,
    NULL AS pct_30_days,
    NULL AS pct_60_days,
    NULL AS pct_90_days,

    cohort_month,
    SUM(has_second_purchase)::float / COUNT(customer_id) AS second_purchase_rate_by_cohort,
    COUNT(customer_id) AS total_customers_in_cohort

FROM
    FinalAnalysis
GROUP BY
    cohort_month
ORDER BY
    cohort_month;

-- NOTA: A Correlação com LTV (Lifetime Value) exigiria a inclusão
-- de outra tabela de dados de pedidos (como 'olist_order_payments_dataset') 
-- para calcular o LTV de cada cliente. Isso não pode ser feito apenas
-- com o `olist_orders_dataset`.