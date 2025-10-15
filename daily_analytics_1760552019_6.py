-- daily_analytics_1760552019_6.py
-- Analytics SQL Queries
-- Generated: 2025-10-15 23:43:33
-- Author: Shivam Sahu

-- Create analytics table
CREATE TABLE IF NOT EXISTS analytics_data (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(255) NOT NULL,
    metric_value DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO analytics_data (metric_name, metric_value) VALUES
('Total Users', 1000),
('Active Sessions', 500),
('Revenue', 10000.50);

-- Query for recent data
SELECT * FROM analytics_data 
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY created_at DESC;
