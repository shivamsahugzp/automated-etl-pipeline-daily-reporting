-- advanced_1760548220_3.sql
-- Advanced Analytics SQL Queries
-- Generated: 2025-10-15 22:40:17
-- Author: Shivam Sahu
-- Description: Complex analytical queries with performance optimization

-- Create optimized analytics tables with partitioning
CREATE TABLE IF NOT EXISTS analytics_data_partitioned (
    id BIGSERIAL,
    metric_name VARCHAR(255) NOT NULL,
    metric_value DECIMAL(15,4),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Create monthly partitions
CREATE TABLE analytics_data_2024_01 PARTITION OF analytics_data_partitioned
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE analytics_data_2024_02 PARTITION OF analytics_data_partitioned
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- Create advanced indexes for performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_analytics_metric_name_btree 
    ON analytics_data_partitioned USING btree (metric_name);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_analytics_category_hash 
    ON analytics_data_partitioned USING hash (category);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_analytics_created_at_brin 
    ON analytics_data_partitioned USING brin (created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_analytics_metadata_gin 
    ON analytics_data_partitioned USING gin (metadata);

-- Insert sample data with realistic values
INSERT INTO analytics_data_partitioned (metric_name, metric_value, category, subcategory, metadata) VALUES
('Total Revenue', 125000.50, 'Financial', 'Sales', '{"currency": "USD", "region": "North America"}'),
('Active Users', 2500, 'User Engagement', 'Daily Active', '{"platform": "Web", "device": "Desktop"}'),
('Conversion Rate', 3.25, 'Marketing', 'E-commerce', '{"campaign": "Summer Sale", "channel": "Email"}'),
('Average Session Duration', 180.75, 'User Behavior', 'Engagement', '{"session_type": "Authenticated"}'),
('Page Views', 15000, 'Traffic', 'Organic', '{"source": "Google", "medium": "Organic"}'),
('Bounce Rate', 45.2, 'User Behavior', 'Engagement', '{"page_type": "Landing"}'),
('Customer Lifetime Value', 1250.75, 'Financial', 'Customer', '{"segment": "Premium"}'),
('Churn Rate', 5.8, 'Customer', 'Retention', '{"period": "Monthly"}');

-- Advanced analytical view with window functions
CREATE OR REPLACE VIEW analytics_summary_advanced AS
SELECT 
    category,
    subcategory,
    COUNT(*) as metric_count,
    AVG(metric_value) as avg_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY metric_value) as median_value,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY metric_value) as p95_value,
    MAX(metric_value) as max_value,
    MIN(metric_value) as min_value,
    SUM(metric_value) as total_value,
    STDDEV(metric_value) as std_deviation,
    VARIANCE(metric_value) as variance,
    -- Window functions for trend analysis
    LAG(metric_value, 1) OVER (PARTITION BY category ORDER BY created_at) as prev_value,
    LEAD(metric_value, 1) OVER (PARTITION BY category ORDER BY created_at) as next_value,
    -- Calculate growth rate
    CASE 
        WHEN LAG(metric_value, 1) OVER (PARTITION BY category ORDER BY created_at) > 0 
        THEN ((metric_value - LAG(metric_value, 1) OVER (PARTITION BY category ORDER BY created_at)) / 
              LAG(metric_value, 1) OVER (PARTITION BY category ORDER BY created_at)) * 100
        ELSE NULL 
    END as growth_rate_percent
FROM analytics_data_partitioned
WHERE metric_value IS NOT NULL
GROUP BY category, subcategory, metric_value, created_at;

-- Complex analytical query with CTEs
WITH monthly_trends AS (
    SELECT 
        DATE_TRUNC('month', created_at) as month,
        category,
        AVG(metric_value) as avg_monthly_value,
        COUNT(*) as monthly_count
    FROM analytics_data_partitioned
    WHERE created_at >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY DATE_TRUNC('month', created_at), category
),
category_performance AS (
    SELECT 
        category,
        AVG(avg_monthly_value) as overall_avg,
        STDDEV(avg_monthly_value) as volatility,
        COUNT(DISTINCT month) as active_months
    FROM monthly_trends
    GROUP BY category
)
SELECT 
    cp.category,
    cp.overall_avg,
    cp.volatility,
    cp.active_months,
    -- Performance ranking
    RANK() OVER (ORDER BY cp.overall_avg DESC) as performance_rank,
    -- Volatility assessment
    CASE 
        WHEN cp.volatility < 100 THEN 'Low Volatility'
        WHEN cp.volatility < 500 THEN 'Medium Volatility'
        ELSE 'High Volatility'
    END as volatility_category
FROM category_performance cp
ORDER BY cp.overall_avg DESC;

-- Performance monitoring query
EXPLAIN (ANALYZE, BUFFERS) 
SELECT 
    metric_name,
    AVG(metric_value) as avg_value,
    COUNT(*) as record_count
FROM analytics_data_partitioned
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    AND category = 'Financial'
GROUP BY metric_name
ORDER BY avg_value DESC;
