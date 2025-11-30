-- second_purchase.sql
-- Time-to-Second-Purchase analysis for olist_orders_dataset
-- Assumptions:
--   - Table: olist_orders_dataset(order_id, customer_id, order_purchase_timestamp)
--   - order_purchase_timestamp is a timestamp or timestamptz
-- PostgreSQL syntax used.

-- 1) CTE with purchase number per customer (chronological)
WITH orders_ranked AS (
    SELECT
        order_id,
        customer_id,
        order_purchase_timestamp::timestamp AS order_purchase_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp::timestamp) AS purchase_number
    FROM olist_orders_dataset
),

-- 2) First and second purchase dates per customer
first_second AS (
    SELECT
        customer_id,
        MIN(order_purchase_ts) FILTER (WHERE purchase_number = 1) AS first_purchase_date,
        MIN(order_purchase_ts) FILTER (WHERE purchase_number = 2) AS second_purchase_date,
        COUNT(*) AS total_purchases
    FROM orders_ranked
    GROUP BY customer_id
),

-- 3) Derived metrics per customer
customer_second_metrics AS (
    SELECT
        customer_id,
        first_purchase_date,
        second_purchase_date,
        total_purchases,
        CASE
            WHEN second_purchase_date IS NOT NULL THEN 1
            ELSE 0
        END AS has_second_purchase,
        -- days to second purchase as integer days (null if no second purchase)
        CASE
            WHEN second_purchase_date IS NOT NULL
            THEN (second_purchase_date::date - first_purchase_date::date)
            ELSE NULL
        END AS days_to_second_purchase,
        -- cohort month of first purchase (YYYY-MM)
        to_char(date_trunc('month', first_purchase_date), 'YYYY-MM') AS cohort_month,
        -- days_bin
        CASE
            WHEN second_purchase_date IS NULL THEN 'no_second_purchase'
            WHEN (second_purchase_date::date - first_purchase_date::date) BETWEEN 0 AND 30 THEN '0-30'
            WHEN (second_purchase_date::date - first_purchase_date::date) BETWEEN 31 AND 60 THEN '31-60'
            WHEN (second_purchase_date::date - first_purchase_date::date) BETWEEN 61 AND 90 THEN '61-90'
            WHEN (second_purchase_date::date - first_purchase_date::date) BETWEEN 91 AND 180 THEN '91-180'
            WHEN (second_purchase_date::date - first_purchase_date::date) > 180 THEN '180+'
            ELSE 'unknown'
        END AS days_bin
    FROM first_second
)

-- Optional: create a view for reuse (uncomment to persist)
-- CREATE OR REPLACE VIEW vw_customer_second_metrics AS
-- SELECT * FROM customer_second_metrics;

-- 4) Overall second-purchase rate
SELECT
    COUNT(*) AS total_customers,
    SUM(has_second_purchase) AS customers_with_second_purchase,
    ROUND(100.0 * SUM(has_second_purchase)::numeric / COUNT(*), 4) AS second_purchase_rate_pct
FROM customer_second_metrics;


-- 5) Time-to-second-purchase statistics (only customers who bought 2+ times)
SELECT
    COUNT(*) AS n_customers_with_second,
    ROUND(AVG(days_to_second_purchase)::numeric, 2) AS mean_days_to_second,
    -- median using percentile_cont
    percentile_cont(0.5) WITHIN GROUP (ORDER BY days_to_second_purchase) AS median_days_to_second,
    MIN(days_to_second_purchase) AS min_days,
    MAX(days_to_second_purchase) AS max_days
FROM customer_second_metrics
WHERE has_second_purchase = 1;


-- 6) Distribution histogram by days_bin (counts and percent among those with a second purchase)
SELECT
    days_bin,
    COUNT(*) AS customers_in_bin,
    ROUND(100.0 * COUNT(*)::numeric / SUM(COUNT(*)) OVER (), 4) AS pct_of_second_purchasers
FROM customer_second_metrics
WHERE has_second_purchase = 1
GROUP BY days_bin
ORDER BY
    -- order bins in logical order
    CASE days_bin
        WHEN '0-30' THEN 1
        WHEN '31-60' THEN 2
        WHEN '61-90' THEN 3
        WHEN '91-180' THEN 4
        WHEN '180+' THEN 5
        ELSE 99
    END;


-- 7) Second-purchase rate by cohort_month (cohort = month of first purchase)
SELECT
    cohort_month,
    COUNT(*) AS cohort_customers,
    SUM(has_second_purchase) AS cohort_customers_with_2plus,
    ROUND(100.0 * SUM(has_second_purchase)::numeric / COUNT(*), 4) AS cohort_second_purchase_rate_pct,
    ROUND(AVG(days_to_second_purchase) FILTER (WHERE has_second_purchase = 1)::numeric, 2) AS cohort_mean_days_to_second,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY days_to_second_purchase) FILTER (WHERE has_second_purchase = 1) AS cohort_median_days_to_second
FROM customer_second_metrics
GROUP BY cohort_month
ORDER BY cohort_month;


-- 8) Critical windows: % that buy in first 30 / 60 / 90 days (overall)
SELECT
    COUNT(*) AS total_customers,
    SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 30 THEN 1 ELSE 0 END) AS within_30_days,
    ROUND(100.0 * SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 30 THEN 1 ELSE 0 END)::numeric / COUNT(*), 4) AS pct_within_30_days,
    SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 60 THEN 1 ELSE 0 END) AS within_60_days,
    ROUND(100.0 * SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 60 THEN 1 ELSE 0 END)::numeric / COUNT(*), 4) AS pct_within_60_days,
    SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 90 THEN 1 ELSE 0 END) AS within_90_days,
    ROUND(100.0 * SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 90 THEN 1 ELSE 0 END)::numeric / COUNT(*), 4) AS pct_within_90_days
FROM customer_second_metrics;


-- 9) Critical windows by cohort_month (same logic, grouped by cohort)
SELECT
    cohort_month,
    COUNT(*) AS cohort_customers,
    SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 30 THEN 1 ELSE 0 END) AS within_30_days,
    ROUND(100.0 * SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 30 THEN 1 ELSE 0 END)::numeric / COUNT(*), 4) AS pct_within_30_days,
    SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 60 THEN 1 ELSE 0 END) AS within_60_days,
    ROUND(100.0 * SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 60 THEN 1 ELSE 0 END)::numeric / COUNT(*), 4) AS pct_within_60_days,
    SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 90 THEN 1 ELSE 0 END) AS within_90_days,
    ROUND(100.0 * SUM(CASE WHEN has_second_purchase = 1 AND days_to_second_purchase <= 90 THEN 1 ELSE 0 END)::numeric / COUNT(*), 4) AS pct_within_90_days
FROM customer_second_metrics
GROUP BY cohort_month
ORDER BY cohort_month;


-- 10) Correlação com LTV
-- NOTE: you need a table customer_ltv(customer_id, ltv_value). If not available, skip this block.
-- This block shows two correlations:
--   a) corr between days_to_second_purchase and ltv_value (only customers with second purchase)
--   b) corr between has_second_purchase (0/1) and ltv_value (all customers)
-- Replace `customer_ltv` and `ltv_value` with your actual table/column names.

-- Uncomment and run if you have customer_ltv:
-- SELECT
--     -- correlation between days to second purchase and LTV (only where both exist)
--     corr(csm.days_to_second_purchase::double precision, cl.ltv_value::double precision) AS corr_days_ltv,
--     -- correlation between having a second purchase and LTV (binary vs LTV)
--     corr(csm.has_second_purchase::double precision, cl.ltv_value::double precision) AS corr_has2_ltv,
--     COUNT(*) FILTER (WHERE csm.has_second_purchase = 1 AND cl.ltv_value IS NOT NULL) AS n_with_days_and_ltv,
--     COUNT(*) FILTER (WHERE cl.ltv_value IS NOT NULL) AS n_with_ltv
-- FROM customer_second_metrics csm
-- JOIN customer_ltv cl USING (customer_id);

-- 11) (Opcional) tabela final detalhada por cliente para export/visualização
SELECT
    customer_id,
    first_purchase_date,
    second_purchase_date,
    total_purchases,
    has_second_purchase,
    days_to_second_purchase,
    cohort_month,
    days_bin
FROM customer_second_metrics
ORDER BY first_purchase_date
LIMIT 500; -- remover limite conforme necessidade
