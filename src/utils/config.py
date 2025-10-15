"""
Configuration management for the ETL pipeline
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional


class Config:
    """
    Configuration manager for the ETL pipeline
    Handles loading and accessing configuration from YAML files
    """
    
    def __init__(self, config_path: str):
        """
        Initialize configuration manager
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config_path = Path(config_path)
        self.config_data = self._load_config()
        self._load_environment_variables()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
            return config or {}
        except FileNotFoundError:
            print(f"Configuration file not found: {self.config_path}")
            return {}
        except yaml.YAMLError as e:
            print(f"Error parsing configuration file: {e}")
            return {}
    
    def _load_environment_variables(self):
        """Load environment variables and substitute in config"""
        if 'database' in self.config_data:
            db_config = self.config_data['database']
            for key in ['password', 'user', 'host']:
                if key in db_config and isinstance(db_config[key], str) and db_config[key].startswith('${'):
                    env_var = db_config[key][2:-1]  # Remove ${ and }
                    db_config[key] = os.getenv(env_var, db_config[key])
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key
        
        Args:
            key (str): Configuration key (supports dot notation)
            default (Any): Default value if key not found
            
        Returns:
            Any: Configuration value
        """
        keys = key.split('.')
        value = self.config_data
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return self.get('database', {})
    
    def get_sources_config(self) -> list:
        """Get data sources configuration"""
        return self.get('sources', [])
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """Get pipeline configuration"""
        return self.get('pipeline', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.get('logging', {})
    
    def is_development(self) -> bool:
        """Check if running in development mode"""
        return self.get('environment', 'development') == 'development'
    
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return self.get('environment', 'development') == 'production'
