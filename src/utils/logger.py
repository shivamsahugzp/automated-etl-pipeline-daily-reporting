"""
Logging configuration for the ETL pipeline
"""

import logging
import sys
from pathlib import Path
from typing import Dict, Any
from loguru import logger


def setup_logger(config: Dict[str, Any]) -> logging.Logger:
    """
    Setup logging configuration for the ETL pipeline
    
    Args:
        config (Dict[str, Any]): Logging configuration
        
    Returns:
        logging.Logger: Configured logger
    """
    # Remove default loguru handler
    logger.remove()
    
    # Get configuration values
    log_level = config.get('level', 'INFO')
    log_format = config.get('format', 
        '<green>{time:YYYY-MM-DD HH:mm:ss}</green> | '
        '<level>{level: <8}</level> | '
        '<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | '
        '<level>{message}</level>'
    )
    
    # Console logging
    logger.add(
        sys.stdout,
        format=log_format,
        level=log_level,
        colorize=True
    )
    
    # File logging
    log_file = config.get('file', 'logs/etl_pipeline.log')
    log_file_path = Path(log_file)
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.add(
        log_file,
        format=log_format,
        level=log_level,
        rotation=config.get('rotation', '1 day'),
        retention=config.get('retention', '30 days'),
        compression=config.get('compression', 'zip')
    )
    
    # Error file logging
    error_log_file = config.get('error_file', 'logs/etl_errors.log')
    error_log_path = Path(error_log_file)
    error_log_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.add(
        error_log_path,
        format=log_format,
        level='ERROR',
        rotation=config.get('rotation', '1 day'),
        retention=config.get('retention', '30 days'),
        compression=config.get('compression', 'zip')
    )
    
    # Performance logging
    perf_log_file = config.get('performance_file', 'logs/etl_performance.log')
    perf_log_path = Path(perf_log_file)
    perf_log_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.add(
        perf_log_path,
        format=log_format,
        level='INFO',
        filter=lambda record: record['extra'].get('performance', False),
        rotation=config.get('rotation', '1 day'),
        retention=config.get('retention', '30 days'),
        compression=config.get('compression', 'zip')
    )
    
    return logger


class PerformanceLogger:
    """Performance logging utility"""
    
    def __init__(self, logger_instance):
        self.logger = logger_instance
    
    def log_performance(self, operation: str, duration: float, records: int = None):
        """
        Log performance metrics
        
        Args:
            operation (str): Operation name
            duration (float): Duration in seconds
            records (int): Number of records processed
        """
        message = f"Performance: {operation} completed in {duration:.2f}s"
        if records:
            message += f" ({records} records, {records/duration:.0f} records/sec)"
        
        self.logger.bind(performance=True).info(message)


def get_logger(name: str) -> logging.Logger:
    """
    Get logger instance for a specific module
    
    Args:
        name (str): Logger name
        
    Returns:
        logging.Logger: Logger instance
    """
    return logger.bind(module=name)
