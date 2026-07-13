-- queries.sql
-- The analytical layer. Everything here is meant to be run against ecommerce.db
-- once clean_data.py has populated it. Grouped by theme, roughly easiest to
-- hardest so it reads like a story rather than a random dump of SQL.
--
-- Note: order revenue only counts orders that weren't Cancelled/Returned,
-- since those didn't actually generate money for the business. That filter
-- shows up in almost every query below -- it's the single most important
-- business rule in this whole project.


-- ============================================================
-- 1. BASIC JOIN + AGGREGATION: revenue by product category
-- ============================================================
SELECT
    p.category,
    COUNT(DISTINCT oi.order_id)                         AS orders_with_category,
    SUM(oi.quantity)                                     AS units_sold,
    ROUND(SUM(oi.quantity * oi.unit_price_at_purchase), 2) AS revenue
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
JOIN orders o   ON o.order_id = oi.order_id
WHERE o.status NOT IN ('Cancelled', 'Returned')
GROUP BY p.category
ORDER BY revenue DESC;


-- ============================================================
-- 2. MONTHLY REVENUE TREND (with month-over-month growth via window function)
-- ============================================================
WITH monthly_revenue AS (
    SELECT
        strftime('%Y-%m', o.order_date) AS order_month,
        ROUND(SUM(oi.quantity * oi.unit_price_at_purchase), 2) AS revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.status NOT IN ('Cancelled', 'Returned')
    GROUP BY order_month
)
SELECT
    order_month,
    revenue,
    LAG(revenue) OVER (ORDER BY order_month)                    AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY order_month)) * 100.0
        / NULLIF(LAG(revenue) OVER (ORDER BY order_month), 0), 1
    ) AS mom_growth_pct
FROM monthly_revenue
ORDER BY order_month;


-- ============================================================
-- 3. RUNNING TOTAL / CUMULATIVE REVENUE (window function, no self-join needed)
-- ============================================================
WITH monthly_revenue AS (
    SELECT
        strftime('%Y-%m', o.order_date) AS order_month,
        ROUND(SUM(oi.quantity * oi.unit_price_at_purchase), 2) AS revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.status NOT IN ('Cancelled', 'Returned')
    GROUP BY order_month
)
SELECT
    order_month,
    revenue,
    ROUND(SUM(revenue) OVER (ORDER BY order_month), 2) AS cumulative_revenue
FROM monthly_revenue
ORDER BY order_month;


-- ============================================================
-- 4. TOP 3 BEST-SELLING PRODUCTS PER CATEGORY (window function: RANK)
-- ============================================================
WITH product_sales AS (
    SELECT
        p.category,
        p.product_name,
        SUM(oi.quantity) AS units_sold,
        ROUND(SUM(oi.quantity * oi.unit_price_at_purchase), 2) AS revenue
    FROM order_items oi
    JOIN products p ON p.product_id = oi.product_id
    JOIN orders o   ON o.order_id = oi.order_id
    WHERE o.status NOT IN ('Cancelled', 'Returned')
    GROUP BY p.category, p.product_name
),
ranked AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS rank_in_category
    FROM product_sales
)
SELECT category, product_name, units_sold, revenue, rank_in_category
FROM ranked
WHERE rank_in_category <= 3
ORDER BY category, rank_in_category;


-- ============================================================
-- 5. CUSTOMER SEGMENTATION (RFM-style: Recency, Frequency, Monetary)
-- ============================================================
WITH order_value AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        SUM(oi.quantity * oi.unit_price_at_purchase) AS order_total
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.status NOT IN ('Cancelled', 'Returned')
    GROUP BY o.order_id, o.customer_id, o.order_date
),
rfm_base AS (
    SELECT
        customer_id,
        JULIANDAY((SELECT MAX(order_date) FROM order_value)) - JULIANDAY(MAX(order_date)) AS recency_days,
        COUNT(*)            AS frequency,
        ROUND(SUM(order_total), 2) AS monetary
    FROM order_value
    GROUP BY customer_id
),
scored AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY recency_days DESC) AS recency_score,   -- 4 = most recent
        NTILE(4) OVER (ORDER BY frequency ASC)     AS frequency_score, -- 4 = most frequent
        NTILE(4) OVER (ORDER BY monetary ASC)      AS monetary_score   -- 4 = highest spend
    FROM rfm_base
)
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    recency_score + frequency_score + monetary_score AS rfm_total,
    CASE
        WHEN recency_score + frequency_score + monetary_score >= 10 THEN 'Champion'
        WHEN recency_score + frequency_score + monetary_score >= 7  THEN 'Loyal'
        WHEN recency_score + frequency_score + monetary_score >= 5  THEN 'At Risk'
        ELSE 'Dormant'
    END AS segment
FROM scored
ORDER BY rfm_total DESC;


-- ============================================================
-- 6. COHORT ANALYSIS: monthly retention by signup cohort
-- Classic cohort table -- rows are signup month, columns are "months since
-- signup", cell values are the number of distinct customers who ordered
-- that many months after they signed up.
-- ============================================================
WITH cohorts AS (
    SELECT
        customer_id,
        strftime('%Y-%m', signup_date) AS cohort_month
    FROM customers
),
order_activity AS (
    SELECT DISTINCT
        o.customer_id,
        strftime('%Y-%m', o.order_date) AS activity_month
    FROM orders o
    WHERE o.status NOT IN ('Cancelled', 'Returned')
),
cohort_activity AS (
    SELECT
        c.cohort_month,
        oa.customer_id,
        -- months between cohort month and activity month
        (CAST(strftime('%Y', oa.activity_month || '-01') AS INT) - CAST(strftime('%Y', c.cohort_month || '-01') AS INT)) * 12
        + (CAST(strftime('%m', oa.activity_month || '-01') AS INT) - CAST(strftime('%m', c.cohort_month || '-01') AS INT)) AS month_number
    FROM order_activity oa
    JOIN cohorts c ON c.customer_id = oa.customer_id
),
cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_customers
    FROM cohorts
    GROUP BY cohort_month
)
SELECT
    ca.cohort_month,
    cs.cohort_customers,
    ca.month_number,
    COUNT(DISTINCT ca.customer_id)                                        AS active_customers,
    ROUND(100.0 * COUNT(DISTINCT ca.customer_id) / cs.cohort_customers, 1) AS retention_pct
FROM cohort_activity ca
JOIN cohort_size cs ON cs.cohort_month = ca.cohort_month
WHERE ca.month_number >= 0
GROUP BY ca.cohort_month, ca.month_number
ORDER BY ca.cohort_month, ca.month_number;


-- ============================================================
-- 7. CUSTOMER LIFETIME VALUE LEADERBOARD (join + aggregation + rank)
-- ============================================================
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    COUNT(DISTINCT o.order_id)                              AS total_orders,
    ROUND(SUM(oi.quantity * oi.unit_price_at_purchase), 2)  AS lifetime_value,
    DENSE_RANK() OVER (ORDER BY SUM(oi.quantity * oi.unit_price_at_purchase) DESC) AS value_rank
FROM customers c
JOIN orders o       ON o.customer_id = c.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
WHERE o.status NOT IN ('Cancelled', 'Returned')
GROUP BY c.customer_id, customer_name
ORDER BY lifetime_value DESC
LIMIT 20;


-- ============================================================
-- 8. ORDER STATUS BREAKDOWN + CANCELLATION RATE PER PAYMENT METHOD
-- ============================================================
SELECT
    payment_method,
    COUNT(*)                                                        AS total_orders,
    SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END)           AS cancelled_orders,
    SUM(CASE WHEN status = 'Returned' THEN 1 ELSE 0 END)            AS returned_orders,
    ROUND(100.0 * SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) / COUNT(*), 1) AS cancellation_rate_pct
FROM orders
GROUP BY payment_method
ORDER BY cancellation_rate_pct DESC;


-- ============================================================
-- 9. REPEAT VS ONE-TIME CUSTOMERS (CTE + CASE)
-- ============================================================
WITH order_counts AS (
    SELECT customer_id, COUNT(*) AS n_orders
    FROM orders
    WHERE status NOT IN ('Cancelled', 'Returned')
    GROUP BY customer_id
)
SELECT
    CASE WHEN n_orders = 1 THEN 'One-time buyer' ELSE 'Repeat buyer' END AS customer_type,
    COUNT(*) AS num_customers,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_customers
FROM order_counts
GROUP BY customer_type;


-- ============================================================
-- 10. NEW VS RETURNING CUSTOMER REVENUE PER MONTH
-- (uses a CTE to find each customer's first order month, then classifies
-- every order against that)
-- ============================================================
WITH first_order AS (
    SELECT customer_id, MIN(strftime('%Y-%m', order_date)) AS first_month
    FROM orders
    WHERE status NOT IN ('Cancelled', 'Returned')
    GROUP BY customer_id
),
order_value AS (
    SELECT
        o.customer_id,
        strftime('%Y-%m', o.order_date) AS order_month,
        SUM(oi.quantity * oi.unit_price_at_purchase) AS order_total
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.status NOT IN ('Cancelled', 'Returned')
    GROUP BY o.order_id, o.customer_id, order_month
)
SELECT
    ov.order_month,
    CASE WHEN ov.order_month = fo.first_month THEN 'New' ELSE 'Returning' END AS customer_type,
    ROUND(SUM(ov.order_total), 2) AS revenue
FROM order_value ov
JOIN first_order fo ON fo.customer_id = ov.customer_id
GROUP BY ov.order_month, customer_type
ORDER BY ov.order_month, customer_type;
