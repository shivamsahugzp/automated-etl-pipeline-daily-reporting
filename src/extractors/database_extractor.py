"""
Database extractor for the ETL pipeline
Handles extraction from PostgreSQL and MySQL databases
"""

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from typing import Dict, Any, Optional
import logging
from ..utils.logger import get_logger


class DatabaseExtractor:
    """
    Database extractor class for extracting data from various database sources
    """
    
    def __init__(self, config):
        """
        Initialize database extractor
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.logger = get_logger(__name__)
        self.engines = {}
    
    def _get_engine(self, connection_string: str) -> sa.Engine:
        """
        Get or create database engine
        
        Args:
            connection_string (str): Database connection string
            
        Returns:
            sa.Engine: SQLAlchemy engine
        """
        if connection_string not in self.engines:
            try:
                self.engines[connection_string] = create_engine(connection_string)
                self.logger.info(f"Created database engine for {connection_string}")
            except Exception as e:
                self.logger.error(f"Failed to create database engine: {e}")
                raise
        
        return self.engines[connection_string]
    
    def extract_postgresql(self, source_config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        Extract data from PostgreSQL database
        
        Args:
            source_config (Dict[str, Any]): Source configuration
            
        Returns:
            Optional[pd.DataFrame]: Extracted data as DataFrame
        """
        try:
            connection = source_config['connection']
            query = source_config.get('query', 'SELECT * FROM {}'.format(source_config.get('table', 'default_table')))
            
            self.logger.info(f"Extracting data from PostgreSQL: {source_config['name']}")
            
            engine = self._get_engine(connection)
            
            # Execute query and return DataFrame
            df = pd.read_sql(query, engine)
            
            self.logger.info(f"Successfully extracted {len(df)} records from PostgreSQL")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract from PostgreSQL: {e}")
            return None
    
    def extract_mysql(self, source_config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        Extract data from MySQL database
        
        Args:
            source_config (Dict[str, Any]): Source configuration
            
        Returns:
            Optional[pd.DataFrame]: Extracted data as DataFrame
        """
        try:
            connection = source_config['connection']
            query = source_config.get('query', 'SELECT * FROM {}'.format(source_config.get('table', 'default_table')))
            
            self.logger.info(f"Extracting data from MySQL: {source_config['name']}")
            
            engine = self._get_engine(connection)
            
            # Execute query and return DataFrame
            df = pd.read_sql(query, engine)
            
            self.logger.info(f"Successfully extracted {len(df)} records from MySQL")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract from MySQL: {e}")
            return None
    
    def extract_with_incremental(self, source_config: Dict[str, Any], 
                                last_run_date: str = None) -> Optional[pd.DataFrame]:
        """
        Extract data with incremental loading based on date
        
        Args:
            source_config (Dict[str, Any]): Source configuration
            last_run_date (str): Last run date for incremental loading
            
        Returns:
            Optional[pd.DataFrame]: Extracted data as DataFrame
        """
        try:
            connection = source_config['connection']
            table = source_config.get('table', 'default_table')
            date_column = source_config.get('date_column', 'created_at')
            
            # Build incremental query
            if last_run_date:
                query = f"SELECT * FROM {table} WHERE {date_column} > '{last_run_date}'"
            else:
                query = f"SELECT * FROM {table}"
            
            self.logger.info(f"Extracting incremental data from {source_config['name']}")
            
            engine = self._get_engine(connection)
            df = pd.read_sql(query, engine)
            
            self.logger.info(f"Successfully extracted {len(df)} incremental records")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract incremental data: {e}")
            return None
    
    def extract_with_chunking(self, source_config: Dict[str, Any], 
                             chunk_size: int = 10000) -> Optional[pd.DataFrame]:
        """
        Extract data in chunks for large datasets
        
        Args:
            source_config (Dict[str, Any]): Source configuration
            chunk_size (int): Size of each chunk
            
        Returns:
            Optional[pd.DataFrame]: Extracted data as DataFrame
        """
        try:
            connection = source_config['connection']
            query = source_config.get('query', 'SELECT * FROM {}'.format(source_config.get('table', 'default_table')))
            
            self.logger.info(f"Extracting data in chunks from {source_config['name']}")
            
            engine = self._get_engine(connection)
            
            # Read data in chunks
            chunks = []
            offset = 0
            
            while True:
                chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
                chunk_df = pd.read_sql(chunk_query, engine)
                
                if chunk_df.empty:
                    break
                
                chunks.append(chunk_df)
                offset += chunk_size
                
                self.logger.info(f"Processed chunk {len(chunks)} with {len(chunk_df)} records")
            
            # Combine all chunks
            if chunks:
                df = pd.concat(chunks, ignore_index=True)
                self.logger.info(f"Successfully extracted {len(df)} records in {len(chunks)} chunks")
                return df
            else:
                self.logger.warning("No data found")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Failed to extract data in chunks: {e}")
            return None
    
    def close_connections(self):
        """Close all database connections"""
        for engine in self.engines.values():
            try:
                engine.dispose()
                self.logger.info("Closed database connection")
            except Exception as e:
                self.logger.warning(f"Error closing database connection: {e}")
