-- sql/business_intelligence.sql
/*
Executive Business Intelligence Dashboard Queries
- Key Performance Indicators (KPIs)
- Trend analysis
- Comparative performance
- Predictive metrics
*/

-- EXECUTIVE KPIs DASHBOARD
WITH kpi_data AS (
    SELECT 
        -- Revenue Metrics
        SUM(CASE WHEN o.status != 'cancelled' THEN o.total_amount ELSE 0 END) AS total_revenue,
        SUM(CASE WHEN o.status != 'cancelled' AND o.order_date >= DATE('now', '-30 days') 
             THEN o.total_amount ELSE 0 END) AS last_30d_revenue,
        
        -- Order Metrics
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN o.status != 'cancelled' THEN o.order_id END) AS successful_orders,
        
        -- Customer Metrics
        COUNT(DISTINCT c.customer_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN o.order_date >= DATE('now', '-90 days') 
             THEN c.customer_id END) AS active_customers,
        
        -- Product Metrics
        COUNT(DISTINCT p.product_id) AS total_products,
        COUNT(DISTINCT CASE WHEN p.stock_quantity > 0 THEN p.product_id END) AS in_stock_products,
        
        -- Payment Metrics
        SUM(CASE WHEN p.payment_status = 'completed' THEN p.amount ELSE 0 END) AS successful_payments,
        SUM(CASE WHEN p.payment_status = 'failed' THEN p.amount ELSE 0 END) AS failed_payments
    FROM orders o
    CROSS JOIN customers c
    CROSS JOIN products p
    CROSS JOIN payments p
    WHERE o.order_id IS NOT NULL
)
SELECT 
    total_revenue,
    last_30d_revenue,
    total_orders,
    successful_orders,
    total_customers,
    active_customers,
    total_products,
    in_stock_products,
    successful_payments,
    failed_payments,
    ROUND(total_revenue / NULLIF(total_orders, 0), 2) AS avg_order_value,
    ROUND(total_revenue / NULLIF(total_customers, 0), 2) AS revenue_per_customer,
    ROUND(active_customers * 100.0 / NULLIF(total_customers, 0), 2) AS active_customer_rate,
    ROUND(failed_payments * 100.0 / NULLIF(successful_payments + failed_payments, 0), 2) AS payment_failure_rate
FROM kpi_data;

-- SALES PERFORMANCE BY CATEGORY & TIER
SELECT 
    p.category,
    c.customer_tier,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT o.customer_id) AS customer_count,
    SUM(oi.quantity) AS total_units_sold,
    SUM(oi.subtotal) AS total_revenue,
    SUM(oi.quantity * (p.price - p.cost_price)) AS total_profit,
    ROUND(SUM(oi.quantity * (p.price - p.cost_price)) * 100.0 / NULLIF(SUM(oi.subtotal), 0), 2) AS profit_margin_percent,
    ROUND(AVG(oi.subtotal), 2) AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.status != 'cancelled'
GROUP BY p.category, c.customer_tier
ORDER BY total_revenue DESC;

-- CUSTOMER GEOGRAPHY ANALYSIS
SELECT 
    c.country,
    c.city,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.total_amount) AS total_revenue,
    ROUND(AVG(o.total_amount), 2) AS avg_order_value,
    ROUND(SUM(o.total_amount) / NULLIF(COUNT(DISTINCT c.customer_id), 0), 2) AS revenue_per_customer
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status != 'cancelled'
GROUP BY c.country, c.city
HAVING customer_count >= 5
ORDER BY total_revenue DESC;