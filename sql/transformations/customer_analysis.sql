-- Customer Analysis Transformation Query
-- This query performs comprehensive customer analysis including RFM analysis
-- and customer segmentation for marketing strategies

WITH customer_orders AS (
    -- Calculate customer order metrics
    SELECT 
        customer_id,
        COUNT(*) as total_orders,
        SUM(order_value) as total_spent,
        AVG(order_value) as avg_order_value,
        MAX(order_date) as last_order_date,
        MIN(order_date) as first_order_date,
        SUM(quantity) as total_quantity,
        COUNT(DISTINCT product_id) as unique_products_purchased
    FROM staging_sales
    WHERE order_date >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY customer_id
),

customer_rfm AS (
    -- Calculate RFM (Recency, Frequency, Monetary) scores
    SELECT 
        customer_id,
        total_orders,
        total_spent,
        avg_order_value,
        last_order_date,
        first_order_date,
        total_quantity,
        unique_products_purchased,
        
        -- Recency: Days since last order
        EXTRACT(DAYS FROM (CURRENT_DATE - last_order_date)) as recency_days,
        
        -- Frequency: Total number of orders
        total_orders as frequency,
        
        -- Monetary: Total amount spent
        total_spent as monetary,
        
        -- RFM Scores (1-5 scale)
        CASE 
            WHEN EXTRACT(DAYS FROM (CURRENT_DATE - last_order_date)) <= 30 THEN 5
            WHEN EXTRACT(DAYS FROM (CURRENT_DATE - last_order_date)) <= 60 THEN 4
            WHEN EXTRACT(DAYS FROM (CURRENT_DATE - last_order_date)) <= 90 THEN 3
            WHEN EXTRACT(DAYS FROM (CURRENT_DATE - last_order_date)) <= 180 THEN 2
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
            WHEN total_spent >= 10000 THEN 5
            WHEN total_spent >= 5000 THEN 4
            WHEN total_spent >= 2000 THEN 3
            WHEN total_spent >= 500 THEN 2
            ELSE 1
        END as monetary_score
        
    FROM customer_orders
),

customer_segments AS (
    -- Create customer segments based on RFM scores
    SELECT 
        *,
        CONCAT(recency_score, frequency_score, monetary_score) as rfm_score,
        
        -- Customer Segments
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Potential Loyalists'
            WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'New Customers'
            WHEN recency_score >= 3 AND frequency_score >= 2 AND monetary_score >= 3 THEN 'Promising'
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Need Attention'
            WHEN recency_score <= 2 AND frequency_score >= 2 AND monetary_score >= 2 THEN 'About to Sleep'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 2 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Cannot Lose Them'
            ELSE 'Others'
        END as customer_segment,
        
        -- Customer Value Tier
        CASE 
            WHEN total_spent >= 10000 THEN 'Premium'
            WHEN total_spent >= 5000 THEN 'High Value'
            WHEN total_spent >= 2000 THEN 'Medium Value'
            WHEN total_spent >= 500 THEN 'Low Value'
            ELSE 'New/Inactive'
        END as value_tier,
        
        -- Purchase Behavior
        CASE 
            WHEN total_orders >= 15 AND avg_order_value >= 500 THEN 'Frequent High-Value'
            WHEN total_orders >= 15 AND avg_order_value < 500 THEN 'Frequent Low-Value'
            WHEN total_orders < 15 AND avg_order_value >= 500 THEN 'Occasional High-Value'
            ELSE 'Occasional Low-Value'
        END as purchase_behavior
        
    FROM customer_rfm
),

customer_lifetime_value AS (
    -- Calculate Customer Lifetime Value (CLV)
    SELECT 
        *,
        -- Simple CLV calculation: Average Order Value * Purchase Frequency * Customer Lifespan
        avg_order_value * total_orders * 
        EXTRACT(DAYS FROM (CURRENT_DATE - first_order_date)) / 365.0 as estimated_clv,
        
        -- Customer Age in days
        EXTRACT(DAYS FROM (CURRENT_DATE - first_order_date)) as customer_age_days,
        
        -- Purchase frequency (orders per month)
        total_orders / GREATEST(EXTRACT(DAYS FROM (CURRENT_DATE - first_order_date)) / 30.0, 1) as monthly_purchase_frequency
        
    FROM customer_segments
)

-- Final output with all customer analysis metrics
SELECT 
    customer_id,
    total_orders,
    total_spent,
    avg_order_value,
    last_order_date,
    first_order_date,
    total_quantity,
    unique_products_purchased,
    recency_days,
    frequency,
    monetary,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_score,
    customer_segment,
    value_tier,
    purchase_behavior,
    estimated_clv,
    customer_age_days,
    monthly_purchase_frequency,
    
    -- Additional calculated fields
    CASE 
        WHEN recency_days <= 30 THEN 'Active'
        WHEN recency_days <= 90 THEN 'At Risk'
        WHEN recency_days <= 180 THEN 'Inactive'
        ELSE 'Lost'
    END as activity_status,
    
    -- Growth potential score (1-10)
    CASE 
        WHEN customer_segment IN ('Champions', 'Loyal Customers') THEN 10
        WHEN customer_segment IN ('Potential Loyalists', 'Promising') THEN 8
        WHEN customer_segment = 'Need Attention' THEN 6
        WHEN customer_segment = 'About to Sleep' THEN 4
        WHEN customer_segment = 'At Risk' THEN 2
        ELSE 1
    END as growth_potential_score,
    
    -- Current timestamp
    CURRENT_TIMESTAMP as analysis_timestamp

FROM customer_lifetime_value
ORDER BY total_spent DESC, last_order_date DESC;
