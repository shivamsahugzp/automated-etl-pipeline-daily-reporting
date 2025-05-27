-- advanced_1760548177_9.sql
-- Machine Learning Data Preparation SQL
-- Generated: 2025-10-15 22:39:28
-- Author: Shivam Sahu
-- Description: SQL queries for ML feature engineering and data preparation

-- Create ML features table
CREATE TABLE IF NOT EXISTS ml_features (
    id BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    feature_name VARCHAR(100) NOT NULL,
    feature_value DECIMAL(15,6),
    feature_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, feature_name, created_at)
);

-- Create indexes for ML queries
CREATE INDEX IF NOT EXISTS idx_ml_features_customer ON ml_features(customer_id);
CREATE INDEX IF NOT EXISTS idx_ml_features_type ON ml_features(feature_type);
CREATE INDEX IF NOT EXISTS idx_ml_features_name ON ml_features(feature_name);

-- Feature engineering queries
WITH customer_metrics AS (
    SELECT 
        customer_id,
        -- Recency features
        EXTRACT(DAYS FROM (CURRENT_DATE - MAX(order_date))) as days_since_last_order,
        EXTRACT(DAYS FROM (CURRENT_DATE - MIN(order_date))) as customer_age_days,
        
        -- Frequency features
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT DATE_TRUNC('month', order_date)) as active_months,
        
        -- Monetary features
        SUM(order_value) as total_spent,
        AVG(order_value) as avg_order_value,
        MAX(order_value) as max_order_value,
        
        -- Behavioral features
        COUNT(DISTINCT product_category) as unique_categories,
        COUNT(DISTINCT payment_method) as payment_methods_used,
        
        -- Temporal features
        EXTRACT(HOUR FROM AVG(order_time)) as avg_order_hour,
        COUNT(CASE WHEN EXTRACT(DOW FROM order_date) IN (0, 6) THEN 1 END) as weekend_orders,
        
        -- Calculated features
        CASE 
            WHEN COUNT(DISTINCT order_id) > 0 
            THEN SUM(order_value) / COUNT(DISTINCT order_id)
            ELSE 0 
        END as revenue_per_order,
        
        CASE 
            WHEN COUNT(DISTINCT DATE_TRUNC('month', order_date)) > 0 
            THEN COUNT(DISTINCT order_id) / COUNT(DISTINCT DATE_TRUNC('month', order_date))
            ELSE 0 
        END as orders_per_month
        
    FROM customer_orders
    WHERE order_date >= CURRENT_DATE - INTERVAL '2 years'
    GROUP BY customer_id
),
feature_normalization AS (
    SELECT 
        customer_id,
        -- Normalize features using z-score
        (days_since_last_order - AVG(days_since_last_order) OVER()) / 
        STDDEV(days_since_last_order) OVER() as recency_zscore,
        
        (total_orders - AVG(total_orders) OVER()) / 
        STDDEV(total_orders) OVER() as frequency_zscore,
        
        (total_spent - AVG(total_spent) OVER()) / 
        STDDEV(total_spent) OVER() as monetary_zscore,
        
        -- Create categorical features
        CASE 
            WHEN days_since_last_order <= 30 THEN 'Recent'
            WHEN days_since_last_order <= 90 THEN 'Moderate'
            WHEN days_since_last_order <= 180 THEN 'Distant'
            ELSE 'Very_Distant'
        END as recency_category,
        
        CASE 
            WHEN total_orders >= 20 THEN 'High_Frequency'
            WHEN total_orders >= 10 THEN 'Medium_Frequency'
            WHEN total_orders >= 5 THEN 'Low_Frequency'
            ELSE 'Very_Low_Frequency'
        END as frequency_category,
        
        CASE 
            WHEN total_spent >= 1000 THEN 'High_Value'
            WHEN total_spent >= 500 THEN 'Medium_Value'
            WHEN total_spent >= 100 THEN 'Low_Value'
            ELSE 'Very_Low_Value'
        END as value_category
        
    FROM customer_metrics
)
-- Insert engineered features
INSERT INTO ml_features (customer_id, feature_name, feature_value, feature_type)
SELECT 
    customer_id,
    'recency_zscore',
    recency_zscore,
    'numerical'
FROM feature_normalization
WHERE recency_zscore IS NOT NULL

UNION ALL

SELECT 
    customer_id,
    'frequency_zscore',
    frequency_zscore,
    'numerical'
FROM feature_normalization
WHERE frequency_zscore IS NOT NULL

UNION ALL

SELECT 
    customer_id,
    'monetary_zscore',
    monetary_zscore,
    'numerical'
FROM feature_normalization
WHERE monetary_zscore IS NOT NULL;

-- Create RFM analysis features
WITH rfm_analysis AS (
    SELECT 
        customer_id,
        -- RFM Scores (1-5 scale)
        CASE 
            WHEN days_since_last_order <= 30 THEN 5
            WHEN days_since_last_order <= 60 THEN 4
            WHEN days_since_last_order <= 90 THEN 3
            WHEN days_since_last_order <= 180 THEN 2
            ELSE 1
        END as recency_score,
        
        CASE 
            WHEN total_orders >= 20 THEN 5
            WHEN total_orders >= 15 THEN 4
            WHEN total_orders >= 10 THEN 3
            WHEN total_orders >= 5 THEN 2
            ELSE 1
        END as frequency_score,
        
        CASE 
            WHEN total_spent >= 1000 THEN 5
            WHEN total_spent >= 750 THEN 4
            WHEN total_spent >= 500 THEN 3
            WHEN total_spent >= 250 THEN 2
            ELSE 1
        END as monetary_score
        
    FROM customer_metrics
),
rfm_segments AS (
    SELECT 
        customer_id,
        recency_score,
        frequency_score,
        monetary_score,
        CONCAT(recency_score, frequency_score, monetary_score) as rfm_string,
        -- Customer segments based on RFM
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'New Customers'
            WHEN recency_score >= 3 AND frequency_score >= 2 AND monetary_score >= 2 THEN 'Potential Loyalists'
            WHEN recency_score >= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score >= 2 AND monetary_score >= 2 THEN 'Cannot Lose Them'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Hibernating'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Lost'
            ELSE 'Others'
        END as customer_segment
    FROM rfm_analysis
)
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    AVG(recency_score) as avg_recency,
    AVG(frequency_score) as avg_frequency,
    AVG(monetary_score) as avg_monetary
FROM rfm_segments
GROUP BY customer_segment
ORDER BY customer_count DESC;
