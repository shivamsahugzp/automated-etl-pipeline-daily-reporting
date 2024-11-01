/**
 * advanced_1760548063_3.jsx - Advanced React Component
 * Generated: 2025-10-15 22:37:40
 * Author: Shivam Sahu
 * Description: High-performance React component with hooks and optimization
 */

import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { Chart } from 'chart.js';
import { debounce } from 'lodash';

const AdvancedAnalyticsComponent = ({ data, config, onUpdate }) => {
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);
    const [chartData, setChartData] = useState(null);
    const [performanceMetrics, setPerformanceMetrics] = useState({});
    
    // Memoized calculations for performance
    const processedData = useMemo(() => {
        if (!data) return null;
        
        const startTime = performance.now();
        
        // Complex data processing
        const processed = data.map(item => ({
            ...item,
            calculatedValue: item.value * Math.random() * 100,
            timestamp: new Date(item.timestamp)
        }));
        
        const processingTime = performance.now() - startTime;
        setPerformanceMetrics(prev => ({
            ...prev,
            processingTime: processingTime
        }));
        
        return processed;
    }, [data]);
    
    // Debounced update function
    const debouncedUpdate = useCallback(
        debounce((newData) => {
            if (onUpdate) {
                onUpdate(newData);
            }
        }, 300),
        [onUpdate]
    );
    
    useEffect(() => {
        if (processedData) {
            debouncedUpdate(processedData);
        }
    }, [processedData, debouncedUpdate]);
    
    const renderChart = useCallback(() => {
        if (!chartData) return;
        
        const ctx = document.getElementById('analytics-chart');
        if (ctx) {
            new Chart(ctx, {
                type: 'line',
                data: chartData,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: true,
                            position: 'top'
                        }
                    },
                    scales: {
                        x: {
                            type: 'time',
                            time: {
                                unit: 'day'
                            }
                        },
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        }
    }, [chartData]);
    
    useEffect(() => {
        renderChart();
    }, [renderChart]);
    
    if (isLoading) {
        return <div className="loading-spinner">Loading analytics...</div>;
    }
    
    if (error) {
        return <div className="error-message">Error: {error.message}</div>;
    }
    
    return (
        <div className="advanced-analytics-component">
            <div className="performance-metrics">
                <span>Processing Time: {performanceMetrics.processingTime?.toFixed(2)}ms</span>
            </div>
            <canvas id="analytics-chart" width="800" height="400"></canvas>
        </div>
    );
};

export default AdvancedAnalyticsComponent;
