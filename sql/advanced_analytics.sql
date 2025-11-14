-- sql/advanced_analytics.sql
/*
Advanced E-commerce Analytics Queries
Features:
- Complex joins with optimization
- Window functions for advanced analytics
- Common Table Expressions (CTEs) for readability
- Business metrics and KPIs
*/

-- 1. CUSTOMER LIFETIME VALUE (CLV) ANALYSIS
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        c.customer_tier,
        c.country,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.total_amount) AS total_revenue,
        AVG(o.total_amount) AS avg_order_value,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        JULIANDAY(MAX(o.order_date)) - JULIANDAY(MIN(o.order_date)) AS customer_lifetime_days
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.status != 'cancelled'
    GROUP BY c.customer_id
),
clv_calculation AS (
    SELECT 
        *,
        total_revenue / NULLIF(customer_lifetime_days, 0) * 365 AS estimated_annual_clv,
        CASE 
            WHEN total_orders >= 5 THEN 'VIP'
            WHEN total_orders >= 2 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM customer_metrics
)
SELECT 
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_revenue), 2) AS avg_total_revenue,
    ROUND(AVG(estimated_annual_clv), 2) AS avg_annual_clv,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM clv_calculation
GROUP BY customer_segment
ORDER BY avg_annual_clv DESC;

-- 2. PRODUCT PERFORMANCE & INVENTORY ANALYSIS
WITH product_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.price,
        p.cost_price,
        p.stock_quantity,
        SUM(oi.quantity) AS total_units_sold,
        SUM(oi.subtotal) AS total_revenue,
        SUM(oi.quantity * p.cost_price) AS total_cost,
        SUM(oi.subtotal) - SUM(oi.quantity * p.cost_price) AS total_profit,
        COUNT(DISTINCT oi.order_id) AS unique_orders
    FROM products p
    LEFT JOIN order_items oi ON p.product_id = oi.product_id
    LEFT JOIN orders o ON oi.order_id = o.order_id AND o.status != 'cancelled'
    GROUP BY p.product_id
),
product_metrics AS (
    SELECT 
        *,
        total_revenue / NULLIF(total_units_sold, 0) AS effective_price,
        total_profit / NULLIF(total_units_sold, 0) AS profit_per_unit,
        total_profit / NULLIF(total_revenue, 0) AS profit_margin,
        stock_quantity / NULLIF(MAX(total_units_sold) OVER (PARTITION BY category), 0) * 30 AS days_of_supply
    FROM product_sales
)
SELECT 
    category,
    product_name,
    price,
    total_units_sold,
    total_revenue,
    ROUND(profit_margin * 100, 2) AS profit_margin_percent,
    ROUND(days_of_supply, 1) AS estimated_days_of_supply,
    CASE 
        WHEN profit_margin > 0.4 THEN 'High Margin'
        WHEN profit_margin > 0.2 THEN 'Medium Margin'
        ELSE 'Low Margin'
    END AS margin_category,
    CASE 
        WHEN days_of_supply < 15 THEN 'Low Stock'
        WHEN days_of_supply > 60 THEN 'Overstocked'
        ELSE 'Optimal Stock'
    END AS stock_status
FROM product_metrics
ORDER BY total_revenue DESC
LIMIT 20;

-- 3. SEASONALITY & TIME SERIES ANALYSIS
WITH daily_metrics AS (
    SELECT 
        DATE(o.order_date) AS order_day,
        COUNT(DISTINCT o.order_id) AS daily_orders,
        COUNT(DISTINCT o.customer_id) AS daily_customers,
        SUM(o.total_amount) AS daily_revenue,
        AVG(o.total_amount) AS avg_order_value,
        SUM(p.amount) AS daily_payments,
        SUM(CASE WHEN p.payment_status = 'failed' THEN 1 ELSE 0 END) AS failed_payments
    FROM orders o
    LEFT JOIN payments p ON o.order_id = p.order_id
    WHERE o.status != 'cancelled'
    GROUP BY DATE(o.order_date)
),
rolling_metrics AS (
    SELECT 
        *,
        AVG(daily_revenue) OVER (
            ORDER BY order_day 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS weekly_avg_revenue,
        AVG(daily_orders) OVER (
            ORDER BY order_day 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS weekly_avg_orders,
        LAG(daily_revenue, 7) OVER (ORDER BY order_day) AS revenue_previous_week
    FROM daily_metrics
)
SELECT 
    order_day,
    daily_orders,
    daily_customers,
    daily_revenue,
    ROUND(weekly_avg_revenue, 2) AS weekly_avg_revenue,
    ROUND(avg_order_value, 2) AS avg_order_value,
    ROUND(
        (daily_revenue - revenue_previous_week) / NULLIF(revenue_previous_week, 0) * 100, 
        2
    ) AS weekly_growth_percent,
    ROUND(
        failed_payments * 100.0 / NULLIF(daily_orders, 0), 
        2
    ) AS payment_failure_rate
FROM rolling_metrics
ORDER BY order_day DESC
LIMIT 30;

-- 4. CUSTOMER ACQUISITION & RETENTION ANALYSIS
WITH customer_monthly_activity AS (
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        c.signup_date,
        STRFTIME('%Y-%m', o.order_date) AS order_month,
        COUNT(DISTINCT o.order_id) AS monthly_orders,
        SUM(o.total_amount) AS monthly_spend,
        MIN(STRFTIME('%Y-%m', o.order_date)) OVER (PARTITION BY c.customer_id) AS first_order_month
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.status != 'cancelled'
    GROUP BY c.customer_id, order_month
),
cohort_analysis AS (
    SELECT 
        first_order_month AS cohort_month,
        order_month AS activity_month,
        COUNT(DISTINCT customer_id) AS active_customers,
        SUM(monthly_spend) AS cohort_revenue
    FROM customer_monthly_activity
    GROUP BY first_order_month, order_month
)
SELECT 
    cohort_month,
    activity_month,
    active_customers,
    cohort_revenue,
    ROUND(
        active_customers * 100.0 / FIRST_VALUE(active_customers) OVER (
            PARTITION BY cohort_month 
            ORDER BY activity_month
        ), 
        2
    ) AS retention_rate_percent
FROM cohort_analysis
WHERE cohort_month >= '2023-01'
ORDER BY cohort_month, activity_month;

-- 5. PAYMENT ANALYTICS & FRAUD RISK ASSESSMENT
WITH payment_analysis AS (
    SELECT 
        p.payment_method,
        p.payment_status,
        COUNT(*) AS transaction_count,
        SUM(p.amount) AS total_amount,
        AVG(p.amount) AS avg_transaction_amount,
        AVG(p.risk_score) AS avg_risk_score,
        SUM(CASE WHEN p.risk_score > 0.5 THEN 1 ELSE 0 END) AS high_risk_count
    FROM payments p
    GROUP BY p.payment_method, p.payment_status
),
method_performance AS (
    SELECT 
        payment_method,
        SUM(CASE WHEN payment_status = 'completed' THEN transaction_count ELSE 0 END) AS successful_transactions,
        SUM(CASE WHEN payment_status = 'failed' THEN transaction_count ELSE 0 END) AS failed_transactions,
        SUM(CASE WHEN payment_status = 'refunded' THEN transaction_count ELSE 0 END) AS refunded_transactions,
        SUM(total_amount) AS total_processed_amount,
        AVG(avg_risk_score) AS overall_risk_score
    FROM payment_analysis
    GROUP BY payment_method
)
SELECT 
    payment_method,
    successful_transactions,
    failed_transactions,
    refunded_transactions,
    total_processed_amount,
    ROUND(
        failed_transactions * 100.0 / (successful_transactions + failed_transactions), 
        2
    ) AS failure_rate_percent,
    ROUND(overall_risk_score, 3) AS avg_risk_score,
    CASE 
        WHEN overall_risk_score > 0.3 THEN 'High Risk'
        WHEN overall_risk_score > 0.1 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS risk_category
FROM method_performance
ORDER BY total_processed_amount DESC;