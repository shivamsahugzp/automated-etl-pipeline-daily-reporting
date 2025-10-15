#!/usr/bin/env python3
"""
Automated ETL Pipeline for Daily Reporting
Main entry point for the ETL pipeline execution
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.extractors.database_extractor import DatabaseExtractor
from src.extractors.api_extractor import APIExtractor
from src.extractors.file_extractor import FileExtractor
from src.transformers.sql_transformer import SQLTransformer
from src.transformers.data_validator import DataValidator
from src.loaders.database_loader import DatabaseLoader
from src.loaders.excel_loader import ExcelLoader
from src.utils.config import Config
from src.utils.logger import setup_logger
from src.utils.helpers import create_directories, cleanup_temp_files


class ETLPipeline:
    """
    Main ETL Pipeline class that orchestrates the entire data processing workflow
    """
    
    def __init__(self, config_path: str = "config/development.yaml"):
        """
        Initialize the ETL Pipeline
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config = Config(config_path)
        self.logger = setup_logger(self.config.get('logging', {}))
        
        # Initialize components
        self.extractors = self._initialize_extractors()
        self.transformers = self._initialize_transformers()
        self.loaders = self._initialize_loaders()
        
        # Pipeline metrics
        self.start_time = None
        self.end_time = None
        self.records_processed = 0
        self.errors = []
        
    def _initialize_extractors(self):
        """Initialize data extractors"""
        return {
            'database': DatabaseExtractor(self.config),
            'api': APIExtractor(self.config),
            'file': FileExtractor(self.config)
        }
    
    def _initialize_transformers(self):
        """Initialize data transformers"""
        return {
            'sql': SQLTransformer(self.config),
            'validator': DataValidator(self.config)
        }
    
    def _initialize_loaders(self):
        """Initialize data loaders"""
        return {
            'database': DatabaseLoader(self.config),
            'excel': ExcelLoader(self.config)
        }
    
    def run_pipeline(self, stage: str = None):
        """
        Run the complete ETL pipeline or specific stage
        
        Args:
            stage (str): Specific stage to run ('extract', 'transform', 'load')
        """
        self.start_time = datetime.now()
        self.logger.info(f"Starting ETL Pipeline at {self.start_time}")
        
        try:
            # Create necessary directories
            create_directories(self.config.get('directories', {}))
            
            if stage is None or stage == 'extract':
                self._run_extract_stage()
            
            if stage is None or stage == 'transform':
                self._run_transform_stage()
            
            if stage is None or stage == 'load':
                self._run_load_stage()
            
            self._generate_summary_report()
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            self.errors.append(str(e))
            raise
        finally:
            self.end_time = datetime.now()
            self._cleanup()
    
    def _run_extract_stage(self):
        """Execute data extraction stage"""
        self.logger.info("Starting Extract Stage")
        
        sources = self.config.get('sources', [])
        extracted_data = {}
        
        for source in sources:
            try:
                source_name = source['name']
                source_type = source['type']
                
                self.logger.info(f"Extracting data from {source_name} ({source_type})")
                
                if source_type == 'postgresql':
                    data = self.extractors['database'].extract_postgresql(source)
                elif source_type == 'mysql':
                    data = self.extractors['database'].extract_mysql(source)
                elif source_type == 'api':
                    data = self.extractors['api'].extract(source)
                elif source_type == 'file':
                    data = self.extractors['file'].extract(source)
                else:
                    raise ValueError(f"Unsupported source type: {source_type}")
                
                extracted_data[source_name] = data
                self.records_processed += len(data) if data is not None else 0
                
                self.logger.info(f"Successfully extracted {len(data) if data is not None else 0} records from {source_name}")
                
            except Exception as e:
                error_msg = f"Failed to extract from {source_name}: {str(e)}"
                self.logger.error(error_msg)
                self.errors.append(error_msg)
        
        # Save extracted data to staging
        self._save_staging_data(extracted_data)
        self.logger.info("Extract Stage completed")
    
    def _run_transform_stage(self):
        """Execute data transformation stage"""
        self.logger.info("Starting Transform Stage")
        
        # Load staging data
        staging_data = self._load_staging_data()
        
        # Apply SQL transformations
        transformed_data = {}
        sql_queries = self.config.get('pipeline', {}).get('stages', {}).get('transform', {}).get('sql_queries', [])
        
        for query_path in sql_queries:
            try:
                query_name = Path(query_path).stem
                self.logger.info(f"Executing transformation: {query_name}")
                
                result = self.transformers['sql'].execute_query(query_path, staging_data)
                transformed_data[query_name] = result
                
                self.logger.info(f"Successfully transformed data for {query_name}")
                
            except Exception as e:
                error_msg = f"Failed to transform {query_path}: {str(e)}"
                self.logger.error(error_msg)
                self.errors.append(error_msg)
        
        # Validate transformed data
        self._validate_data(transformed_data)
        
        # Save transformed data
        self._save_transformed_data(transformed_data)
        self.logger.info("Transform Stage completed")
    
    def _run_load_stage(self):
        """Execute data loading stage"""
        self.logger.info("Starting Load Stage")
        
        # Load transformed data
        transformed_data = self._load_transformed_data()
        
        # Load to target database
        target_tables = self.config.get('pipeline', {}).get('stages', {}).get('load', {}).get('target_tables', [])
        
        for table_name in target_tables:
            try:
                self.logger.info(f"Loading data to {table_name}")
                
                # Find corresponding transformed data
                data_key = table_name.replace('fact_', '').replace('dim_', '')
                if data_key in transformed_data:
                    self.loaders['database'].load_table(table_name, transformed_data[data_key])
                    self.logger.info(f"Successfully loaded data to {table_name}")
                else:
                    self.logger.warning(f"No transformed data found for {table_name}")
                
            except Exception as e:
                error_msg = f"Failed to load data to {table_name}: {str(e)}"
                self.logger.error(error_msg)
                self.errors.append(error_msg)
        
        # Generate Excel reports
        self._generate_excel_reports(transformed_data)
        
        self.logger.info("Load Stage completed")
    
    def _save_staging_data(self, data: dict):
        """Save extracted data to staging area"""
        staging_dir = Path(self.config.get('directories', {}).get('staging', 'data/staging'))
        staging_dir.mkdir(parents=True, exist_ok=True)
        
        for source_name, df in data.items():
            if df is not None:
                file_path = staging_dir / f"{source_name}_staging.parquet"
                df.to_parquet(file_path, index=False)
                self.logger.info(f"Saved staging data to {file_path}")
    
    def _load_staging_data(self) -> dict:
        """Load data from staging area"""
        staging_dir = Path(self.config.get('directories', {}).get('staging', 'data/staging'))
        staging_data = {}
        
        for file_path in staging_dir.glob("*_staging.parquet"):
            source_name = file_path.stem.replace("_staging", "")
            df = pd.read_parquet(file_path)
            staging_data[source_name] = df
            self.logger.info(f"Loaded staging data from {file_path}")
        
        return staging_data
    
    def _validate_data(self, data: dict):
        """Validate transformed data"""
        for data_name, df in data.items():
            try:
                validation_results = self.transformers['validator'].validate(df)
                
                if not validation_results['is_valid']:
                    error_msg = f"Data validation failed for {data_name}: {validation_results['errors']}"
                    self.logger.error(error_msg)
                    self.errors.append(error_msg)
                else:
                    self.logger.info(f"Data validation passed for {data_name}")
                    
            except Exception as e:
                error_msg = f"Data validation error for {data_name}: {str(e)}"
                self.logger.error(error_msg)
                self.errors.append(error_msg)
    
    def _save_transformed_data(self, data: dict):
        """Save transformed data"""
        output_dir = Path(self.config.get('directories', {}).get('output', 'data/output'))
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for data_name, df in data.items():
            file_path = output_dir / f"{data_name}_transformed.parquet"
            df.to_parquet(file_path, index=False)
            self.logger.info(f"Saved transformed data to {file_path}")
    
    def _load_transformed_data(self) -> dict:
        """Load transformed data"""
        output_dir = Path(self.config.get('directories', {}).get('output', 'data/output'))
        transformed_data = {}
        
        for file_path in output_dir.glob("*_transformed.parquet"):
            data_name = file_path.stem.replace("_transformed", "")
            df = pd.read_parquet(file_path)
            transformed_data[data_name] = df
            self.logger.info(f"Loaded transformed data from {file_path}")
        
        return transformed_data
    
    def _generate_excel_reports(self, data: dict):
        """Generate Excel reports from transformed data"""
        try:
            self.logger.info("Generating Excel reports")
            self.loaders['excel'].generate_reports(data)
            self.logger.info("Excel reports generated successfully")
        except Exception as e:
            error_msg = f"Failed to generate Excel reports: {str(e)}"
            self.logger.error(error_msg)
            self.errors.append(error_msg)
    
    def _generate_summary_report(self):
        """Generate pipeline summary report"""
        duration = (self.end_time - self.start_time).total_seconds()
        
        summary = {
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'duration_seconds': duration,
            'records_processed': self.records_processed,
            'errors_count': len(self.errors),
            'errors': self.errors,
            'status': 'SUCCESS' if len(self.errors) == 0 else 'FAILED'
        }
        
        self.logger.info(f"Pipeline Summary: {summary}")
        
        # Save summary to file
        summary_file = Path(self.config.get('directories', {}).get('output', 'data/output')) / 'pipeline_summary.json'
        import json
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
    
    def _cleanup(self):
        """Cleanup temporary files and resources"""
        try:
            cleanup_temp_files(self.config.get('directories', {}))
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.warning(f"Cleanup failed: {str(e)}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Automated ETL Pipeline for Daily Reporting')
    parser.add_argument('--config', '-c', default='config/development.yaml',
                       help='Configuration file path')
    parser.add_argument('--stage', '-s', choices=['extract', 'transform', 'load'],
                       help='Run specific ETL stage')
    
    args = parser.parse_args()
    
    try:
        # Initialize and run pipeline
        pipeline = ETLPipeline(args.config)
        pipeline.run_pipeline(args.stage)
        
        print("✅ ETL Pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ ETL Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
