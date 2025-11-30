-- ===================================================================
-- Análise de Time-to-Second-Purchase para o dataset Olist
-- Nome do Arquivo: second_purchase.sql
-- ===================================================================

-- CTE (Common Table Expression) para numerar as compras de cada cliente em ordem cronológica.
-- Isso é fundamental para identificar a primeira e a segunda compra.
WITH ranked_orders AS (
    SELECT
        customer_id,
        order_id,
        order_purchase_timestamp,
        -- A função ROW_NUMBER() atribui um número sequencial para cada pedido do mesmo cliente,
        -- ordenado pela data da compra.
        ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_purchase_timestamp ASC) AS purchase_number
    FROM
        olist_orders_dataset
    -- É uma boa prática ignorar pedidos cancelados ou que não foram entregues,
    -- pois não representam uma compra "concluída".
    WHERE
        order_status NOT IN ('canceled', 'unavailable')
),

-- CTE para isolar a data da primeira compra de cada cliente.
first_purchases AS (
    SELECT
        customer_id,
        order_purchase_timestamp AS first_purchase_date
    FROM
        ranked_orders
    WHERE
        purchase_number = 1
),

-- CTE para isolar a data da segunda compra de cada cliente.
-- Apenas clientes que tiveram uma segunda compra aparecerão aqui.
second_purchases AS (
    SELECT
        customer_id,
        order_purchase_timestamp AS second_purchase_date
    FROM
        ranked_orders
    WHERE
        purchase_number = 2
),

-- CTE principal que une todas as informações.
-- Usamos LEFT JOIN para garantir que TODOS os clientes (mesmo os que não compraram pela segunda vez) sejam incluídos.
final_customer_data AS (
    SELECT
        fp.customer_id,
        fp.first_purchase_date,
        sp.second_purchase_date,
        -- Calcula a diferença em dias entre a primeira e a segunda compra.
        -- A função DATEDIFF pode ter sintaxe diferente dependendo do dialeto SQL (ex: PostgreSQL, SQL Server, BigQuery).
        -- Para PostgreSQL: (sp.second_purchase_date::date - fp.first_purchase_date::date)
        -- Para BigQuery: DATE_DIFF(sp.second_purchase_date, fp.first_purchase_date, DAY)
        -- Usando uma sintaxe comum:
        DATEDIFF(day, fp.first_purchase_date, sp.second_purchase_date) AS days_to_second_purchase,
        
        -- Cria a flag binária: 1 se o cliente tem segunda compra, 0 caso contrário.
        CASE 
            WHEN sp.customer_id IS NOT NULL THEN 1 
            ELSE 0 
        END AS has_second_purchase,

        -- Define o "mês da coorte" do cliente, ou seja, o mês em que ele fez sua primeira compra.
        -- DATE_TRUNC é uma função comum para isso. A sintaxe pode variar.
        -- Para PostgreSQL: DATE_TRUNC('month', fp.first_purchase_date)::DATE
        -- Para SQL Server: DATEFROMPARTS(YEAR(fp.first_purchase_date), MONTH(fp.first_purchase_date), 1)
        -- Para BigQuery: DATE_TRUNC(fp.first_purchase_date, MONTH)
        -- Usando uma sintaxe genérica:
        DATE_FORMAT(fp.first_purchase_date, '%Y-%m-01') AS cohort_month

    FROM
        first_purchases fp
    LEFT JOIN
        second_purchases sp ON fp.customer_id = sp.customer_id
)

-- ===================================================================
-- SEÇÃO 1: MÉTRICAS FINAIS (Descomente a consulta que deseja executar)
-- ===================================================================

-- 1. Taxa de segunda compra geral
/*
SELECT
    -- A média da flag binária nos dá a proporção (ou taxa) de clientes com segunda compra.
    AVG(has_second_purchase) AS overall_second_purchase_rate
FROM
    final_customer_data;
*/

-- 2. Tempo médio e mediano até a segunda compra (apenas para quem comprou novamente)
/*
SELECT
    AVG(days_to_second_purchase) AS average_days_to_second_purchase,
    -- A mediana é mais robusta a outliers. PERCENTILE_CONT(0.5) é o padrão SQL para mediana.
    -- Pode não estar disponível em todos os dialetos SQL mais simples.
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_second_purchase) AS median_days_to_second_purchase
FROM
    final_customer_data
WHERE
    has_second_purchase = 1;
*/

-- 3. Distribuição do tempo (Histograma) usando os bins definidos
/*
SELECT
    CASE
        WHEN days_to_second_purchase BETWEEN 0 AND 30 THEN '0-30 dias'
        WHEN days_to_second_purchase BETWEEN 31 AND 60 THEN '31-60 dias'
        WHEN days_to_second_purchase BETWEEN 61 AND 90 THEN '61-90 dias'
        WHEN days_to_second_purchase BETWEEN 91 AND 180 THEN '91-180 dias'
        WHEN days_to_second_purchase > 180 THEN '180+ dias'
        ELSE 'Sem segunda compra'
    END AS days_bin,
    COUNT(*) AS number_of_customers,
    -- Calcula a porcentagem em relação ao total de clientes
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM final_customer_data), 2) AS percentage_of_total_customers
FROM
    final_customer_data
GROUP BY
    days_bin
ORDER BY
    MIN(days_to_second_purchase);
*/


-- 4. Taxa de segunda compra por coorte (mês da primeira compra)
/*
SELECT
    cohort_month,
    COUNT(*) AS total_customers_in_cohort,
    SUM(has_second_purchase) AS customers_with_second_purchase,
    -- Calcula a taxa para cada coorte
    ROUND(100.0 * SUM(has_second_purchase) / COUNT(*), 2) AS second_purchase_rate_by_cohort
FROM
    final_customer_data
GROUP BY
    cohort_month
ORDER BY
    cohort_month;
*/


-- 5. Janela Crítica (% que compram nos primeiros 30/60/90 dias)
/*
SELECT
    -- Percentual que comprou no primeiro mês
    ROUND(100.0 * AVG(CASE WHEN days_to_second_purchase <= 30 THEN 1.0 ELSE 0.0 END), 2) AS critical_window_30_days,
    -- Percentual que comprou nos primeiros 2 meses
    ROUND(100.0 * AVG(CASE WHEN days_to_second_purchase <= 60 THEN 1.0 ELSE 0.0 END), 2) AS critical_window_60_days,
    -- Percentual que comprou nos primeiros 3 meses
    ROUND(100.0 * AVG(CASE WHEN days_to_second_purchase <= 90 THEN 1.0 ELSE 0.0 END), 2) AS critical_window_90_days
FROM
    final_customer_data;
*/


-- ===================================================================
-- SEÇÃO 2: PREPARAÇÃO DOS DADOS PARA CORRELAÇÃO COM LTV
-- A correlação em si é geralmente calculada em uma ferramenta de análise (Python/R),
-- mas podemos preparar os dados aqui.
-- ===================================================================

-- Para calcular o LTV (Lifetime Value), precisamos de outras tabelas do dataset Olist.
-- Vamos criar uma CTE para calcular o LTV total por cliente.

/*
WITH customer_ltv AS (
    SELECT
        o.customer_id,
        SUM(p.payment_value) AS ltv
    FROM
        olist_orders_dataset o
    JOIN
        olist_order_payments p ON o.order_id = p.order_id
    WHERE
        o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY
        o.customer_id
)

-- Agora, unimos os dados de tempo para a segunda compra com o LTV
SELECT
    fcd.customer_id,
    fcd.days_to_second_purchase,
    ltv.ltv
FROM
    final_customer_data fcd
JOIN
    customer_ltv ON fcd.customer_id = customer_ltv.customer_id
WHERE
    fcd.has_second_purchase = 1; -- Filtramos apenas clientes com segunda compra para a correlação
-- O resultado desta consulta pode ser exportado para um CSV e analisado em Python/R para encontrar a correlação.
*/