-- Sales Summary Transformation Query
-- This query creates comprehensive sales summaries for daily reporting
-- including trends, performance metrics, and key insights

WITH daily_sales AS (
    -- Daily sales aggregation
    SELECT 
        DATE_TRUNC('day', order_date) as sales_date,
        COUNT(*) as total_orders,
        SUM(order_value) as total_revenue,
        AVG(order_value) as avg_order_value,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(quantity) as total_quantity,
        SUM(discount) as total_discount,
        SUM(order_value - discount) as net_revenue,
        
        -- High-value orders (orders > $1000)
        SUM(CASE WHEN order_value > 1000 THEN 1 ELSE 0 END) as high_value_orders,
        SUM(CASE WHEN order_value > 1000 THEN order_value ELSE 0 END) as high_value_revenue,
        
        -- Order status breakdown
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_orders,
        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_orders,
        SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) as cancelled_orders,
        
        -- Revenue by status
        SUM(CASE WHEN status = 'completed' THEN order_value ELSE 0 END) as completed_revenue,
        SUM(CASE WHEN status = 'pending' THEN order_value ELSE 0 END) as pending_revenue,
        SUM(CASE WHEN status = 'cancelled' THEN order_value ELSE 0 END) as cancelled_revenue
        
    FROM staging_sales
    WHERE order_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('day', order_date)
),

weekly_sales AS (
    -- Weekly sales aggregation
    SELECT 
        DATE_TRUNC('week', order_date) as sales_week,
        COUNT(*) as total_orders,
        SUM(order_value) as total_revenue,
        AVG(order_value) as avg_order_value,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(quantity) as total_quantity
    FROM staging_sales
    WHERE order_date >= CURRENT_DATE - INTERVAL '12 weeks'
    GROUP BY DATE_TRUNC('week', order_date)
),

monthly_sales AS (
    -- Monthly sales aggregation
    SELECT 
        DATE_TRUNC('month', order_date) as sales_month,
        COUNT(*) as total_orders,
        SUM(order_value) as total_revenue,
        AVG(order_value) as avg_order_value,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(quantity) as total_quantity
    FROM staging_sales
    WHERE order_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY DATE_TRUNC('month', order_date)
),

product_performance AS (
    -- Product performance metrics
    SELECT 
        product_id,
        COUNT(*) as total_orders,
        SUM(order_value) as total_revenue,
        AVG(order_value) as avg_order_value,
        SUM(quantity) as total_quantity,
        COUNT(DISTINCT customer_id) as unique_customers,
        MAX(order_date) as last_sold_date,
        MIN(order_date) as first_sold_date
    FROM staging_sales
    WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY product_id
),

customer_performance AS (
    -- Customer performance metrics
    SELECT 
        customer_id,
        COUNT(*) as total_orders,
        SUM(order_value) as total_spent,
        AVG(order_value) as avg_order_value,
        MAX(order_date) as last_order_date,
        MIN(order_date) as first_order_date,
        SUM(quantity) as total_quantity
    FROM staging_sales
    WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY customer_id
),

sales_trends AS (
    -- Calculate sales trends and growth rates
    SELECT 
        sales_date,
        total_orders,
        total_revenue,
        avg_order_value,
        unique_customers,
        total_quantity,
        net_revenue,
        high_value_orders,
        high_value_revenue,
        completed_orders,
        pending_orders,
        cancelled_orders,
        completed_revenue,
        pending_revenue,
        cancelled_revenue,
        
        -- Day-over-day growth
        LAG(total_revenue, 1) OVER (ORDER BY sales_date) as prev_day_revenue,
        LAG(total_orders, 1) OVER (ORDER BY sales_date) as prev_day_orders,
        
        -- 7-day moving averages
        AVG(total_revenue) OVER (
            ORDER BY sales_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as revenue_7day_avg,
        AVG(total_orders) OVER (
            ORDER BY sales_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as orders_7day_avg,
        
        -- 30-day moving averages
        AVG(total_revenue) OVER (
            ORDER BY sales_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as revenue_30day_avg,
        AVG(total_orders) OVER (
            ORDER BY sales_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as orders_30day_avg
        
    FROM daily_sales
),

sales_insights AS (
    -- Generate key insights and metrics
    SELECT 
        *,
        
        -- Growth rates
        CASE 
            WHEN prev_day_revenue > 0 THEN 
                ROUND(((total_revenue - prev_day_revenue) / prev_day_revenue) * 100, 2)
            ELSE NULL 
        END as revenue_growth_pct,
        
        CASE 
            WHEN prev_day_orders > 0 THEN 
                ROUND(((total_orders - prev_day_orders) / prev_day_orders) * 100, 2)
            ELSE NULL 
        END as orders_growth_pct,
        
        -- Performance indicators
        CASE 
            WHEN total_revenue > revenue_7day_avg * 1.1 THEN 'Above Average'
            WHEN total_revenue < revenue_7day_avg * 0.9 THEN 'Below Average'
            ELSE 'Average'
        END as revenue_performance,
        
        CASE 
            WHEN total_orders > orders_7day_avg * 1.1 THEN 'Above Average'
            WHEN total_orders < orders_7day_avg * 0.9 THEN 'Below Average'
            ELSE 'Average'
        END as orders_performance,
        
        -- Conversion rates
        ROUND((completed_orders::DECIMAL / NULLIF(total_orders, 0)) * 100, 2) as completion_rate,
        ROUND((high_value_orders::DECIMAL / NULLIF(total_orders, 0)) * 100, 2) as high_value_rate,
        
        -- Customer metrics
        ROUND(total_revenue / NULLIF(unique_customers, 0), 2) as revenue_per_customer,
        ROUND(total_orders::DECIMAL / NULLIF(unique_customers, 0), 2) as orders_per_customer
        
    FROM sales_trends
)

-- Final comprehensive sales summary
SELECT 
    sales_date,
    total_orders,
    total_revenue,
    avg_order_value,
    unique_customers,
    total_quantity,
    net_revenue,
    total_discount,
    high_value_orders,
    high_value_revenue,
    completed_orders,
    pending_orders,
    cancelled_orders,
    completed_revenue,
    pending_revenue,
    cancelled_revenue,
    prev_day_revenue,
    prev_day_orders,
    revenue_7day_avg,
    orders_7day_avg,
    revenue_30day_avg,
    orders_30day_avg,
    revenue_growth_pct,
    orders_growth_pct,
    revenue_performance,
    orders_performance,
    completion_rate,
    high_value_rate,
    revenue_per_customer,
    orders_per_customer,
    
    -- Additional calculated fields
    EXTRACT(DOW FROM sales_date) as day_of_week,
    EXTRACT(MONTH FROM sales_date) as month,
    EXTRACT(QUARTER FROM sales_date) as quarter,
    EXTRACT(YEAR FROM sales_date) as year,
    
    -- Weekday vs Weekend
    CASE 
        WHEN EXTRACT(DOW FROM sales_date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    
    -- Seasonality indicators
    CASE 
        WHEN EXTRACT(MONTH FROM sales_date) IN (11, 12, 1) THEN 'Holiday Season'
        WHEN EXTRACT(MONTH FROM sales_date) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM sales_date) IN (3, 4, 5) THEN 'Spring'
        ELSE 'Fall'
    END as season,
    
    -- Performance score (1-10)
    CASE 
        WHEN revenue_performance = 'Above Average' AND orders_performance = 'Above Average' THEN 10
        WHEN revenue_performance = 'Above Average' OR orders_performance = 'Above Average' THEN 8
        WHEN revenue_performance = 'Average' AND orders_performance = 'Average' THEN 6
        WHEN revenue_performance = 'Below Average' OR orders_performance = 'Below Average' THEN 4
        ELSE 2
    END as performance_score,
    
    -- Current timestamp
    CURRENT_TIMESTAMP as analysis_timestamp

FROM sales_insights
ORDER BY sales_date DESC;
