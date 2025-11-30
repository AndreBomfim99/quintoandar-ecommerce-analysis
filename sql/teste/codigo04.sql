-- CTE para numerar as compras de cada cliente em ordem cronológica
WITH CustomerPurchases AS (
    SELECT
        customer_id,
        order_purchase_timestamp,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp) AS purchase_number
    FROM
        olist_orders_dataset
),

-- CTE para identificar a data da primeira e da segunda compra de cada cliente
PurchaseDates AS (
    SELECT
        customer_id,
        MIN(CASE WHEN purchase_number = 1 THEN order_purchase_timestamp END) AS first_purchase_date,
        MIN(CASE WHEN purchase_number = 2 THEN order_purchase_timestamp END) AS second_purchase_date
    FROM
        CustomerPurchases
    GROUP BY
        customer_id
),

-- CTE para calcular as métricas intermediárias por cliente
SecondPurchaseAnalysis AS (
    SELECT
        customer_id,
        first_purchase_date,
        second_purchase_date,
        -- A função para calcular a diferença de dias pode variar (ex: DATEDIFF, AGE)
        -- Usando JULIANDAY para compatibilidade com SQLite.
        JULIANDAY(second_purchase_date) - JULIANDAY(first_purchase_date) AS days_to_second_purchase,
        CASE WHEN second_purchase_date IS NOT NULL THEN 1 ELSE 0 END AS has_second_purchase,
        -- Formata a data da primeira compra como 'YYYY-MM' para definir o cohort
        STRFTIME('%Y-%m', first_purchase_date) AS cohort_month
    FROM
        PurchaseDates
)

-- Consulta final para agregar as métricas
SELECT
    -- Métrica 1: Taxa de segunda compra geral
    CAST(SUM(has_second_purchase) AS REAL) / COUNT(customer_id) AS second_purchase_rate,

    -- Métrica 2: Tempo médio e mediano até a segunda compra
    AVG(days_to_second_purchase) AS avg_days_to_second_purchase,
    -- O cálculo da mediana (MEDIAN) é mais complexo e depende do dialeto SQL.
    -- Em PostgreSQL, seria: PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_second_purchase)
    -- Em BigQuery: APPROX_QUANTILES(days_to_second_purchase, 2)[OFFSET(1)]
    -- Vamos omitir a mediana por enquanto para manter a compatibilidade.

    -- Métrica 3: Distribuição de tempo (histograma)
    SUM(CASE WHEN days_to_second_purchase BETWEEN 0 AND 30 THEN 1 ELSE 0 END) AS bin_0_30_days,
    SUM(CASE WHEN days_to_second_purchase BETWEEN 31 AND 60 THEN 1 ELSE 0 END) AS bin_31_60_days,
    SUM(CASE WHEN days_to_second_purchase BETWEEN 61 AND 90 THEN 1 ELSE 0 END) AS bin_61_90_days,
    SUM(CASE WHEN days_to_second_purchase BETWEEN 91 AND 180 THEN 1 ELSE 0 END) AS bin_91_180_days,
    SUM(CASE WHEN days_to_second_purchase > 180 THEN 1 ELSE 0 END) AS bin_over_180_days,

    -- Métrica 4: Taxa de segunda compra por cohort (exemplo de agregação)
    -- Para obter a taxa por cohort, você agruparia por 'cohort_month'
    -- Exemplo de como seria essa consulta separadamente:
    /*
    SELECT
        cohort_month,
        COUNT(customer_id) AS total_customers,
        SUM(has_second_purchase) AS customers_with_second_purchase,
        CAST(SUM(has_second_purchase) AS REAL) / COUNT(customer_id) AS cohort_second_purchase_rate
    FROM
        SecondPurchaseAnalysis
    GROUP BY
        cohort_month
    ORDER BY
        cohort_month;
    */

    -- Métrica 5: Janela crítica (exemplo de cálculo)
    CAST(SUM(CASE WHEN days_to_second_purchase <= 30 THEN 1 ELSE 0 END) AS REAL) / SUM(has_second_purchase) AS percent_in_first_30_days,
    CAST(SUM(CASE WHEN days_to_second_purchase <= 60 THEN 1 ELSE 0 END) AS REAL) / SUM(has_second_purchase) AS percent_in_first_60_days,
    CAST(SUM(CASE WHEN days_to_second_purchase <= 90 THEN 1 ELSE 0 END) AS REAL) / SUM(has_second_purchase) AS percent_in_first_90_days

    -- Métrica 6: Correlação com LTV
    -- Esta métrica requer dados de valor do pedido (LTV) e seria calculada
    -- em uma análise estatística separada, possivelmente fora do SQL.
FROM
    SecondPurchaseAnalysis;
