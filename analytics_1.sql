-- analytics_1.sql
-- Data Analytics SQL Queries
-- Generated: 2025-10-15 22:10:28
-- Author: Shivam Sahu

-- Create analytics tables
CREATE TABLE IF NOT EXISTS analytics_data (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(255) NOT NULL,
    metric_value DECIMAL(15,2),
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_analytics_metric_name ON analytics_data(metric_name);
CREATE INDEX IF NOT EXISTS idx_analytics_category ON analytics_data(category);
CREATE INDEX IF NOT EXISTS idx_analytics_created_at ON analytics_data(created_at);

-- Insert sample data
INSERT INTO analytics_data (metric_name, metric_value, category) VALUES
('Total Revenue', 125000.50, 'Financial'),
('Active Users', 2500, 'User Engagement'),
('Conversion Rate', 3.25, 'Marketing'),
('Average Session Duration', 180.75, 'User Behavior'),
('Page Views', 15000, 'Traffic');

-- Create view for summary analytics
CREATE OR REPLACE VIEW analytics_summary AS
SELECT 
    category,
    COUNT(*) as metric_count,
    AVG(metric_value) as avg_value,
    MAX(metric_value) as max_value,
    MIN(metric_value) as min_value,
    SUM(metric_value) as total_value
FROM analytics_data
WHERE metric_value IS NOT NULL
GROUP BY category;

-- Query for recent analytics
SELECT 
    metric_name,
    metric_value,
    category,
    created_at
FROM analytics_data
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY created_at DESC;
