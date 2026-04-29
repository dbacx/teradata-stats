"""
Module 3 Performance Collector

Collects performance data from Teradata system tables for the
Performance & DDL Assessment module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_collector import BaseCollector
from core.config import SYSTEM_DATABASES

# Configure logging
logger = logging.getLogger(__name__)


class PerformanceCollector(BaseCollector):
    """
    Collector for Performance Assessment Module (Module 3).
    
    Collects data from 4 components:
    1. Full Table Scans - Tables with high I/O usage
    2. Highly Skewed Queries - Queries with high CPU skew
    3. Spool Usage Alerts - Queries with spool issues
    4. Unused Indexes - Secondary indexes on large tables for review
    """
    
    def __init__(self):
        """Initialize the Performance Collector."""
        super().__init__(module_name='module_3_performance')
        self.sql_files = [
            '01_full_table_scans.sql',
            '02_highly_skewed_queries.sql',
            '03_spool_usage_alerts.sql',
            '04_unused_indexes.sql'
        ]
        logger.info("Initialized PerformanceCollector")
    
    def collect(self, connection, params: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Collect performance data from Teradata.
        
        Args:
            connection: Database connection object
            params: Optional dictionary with parameters (not used in this module)
        
        Returns:
            Dictionary mapping component names to DataFrames
        """
        # Set default parameters
        if params is None:
            params = {}
        
        # Format system databases for SQL IN clause
        formatted_system_db_list = ", ".join([f"'{db}'" for db in SYSTEM_DATABASES])
        params['system_databases'] = formatted_system_db_list
        
        results = {}
        
        for sql_file in self.sql_files:
            try:
                # Extract component name from filename
                component_name = sql_file.replace('.sql', '')
                
                # Special handling for PDCRINFO views which may not be available
                if component_name in ['01_full_table_scans', '02_highly_skewed_queries', '03_spool_usage_alerts']:
                    try:
                        df = self._collect_with_fallback(connection, sql_file, params)
                        results[component_name] = df
                        logger.info(f"Collected {len(df)} rows for {component_name}")
                    except Exception as e:
                        logger.warning(f"PDCRINFO view not available or permission denied for {component_name}: {str(e)}")
                        # Return empty DataFrame with expected columns
                        results[component_name] = self._get_empty_dataframe_for_component(component_name)
                        logger.info(f"Using fallback empty DataFrame for {component_name}")
                else:
                    # Read SQL file
                    sql = self.read_sql_file(sql_file)
                    
                    # Replace placeholders
                    sql = self.replace_placeholders(sql, params)
                    
                    # Execute query
                    df = self.execute_query(connection, sql)
                    
                    results[component_name] = df
                    logger.info(f"Collected {len(df)} rows for {component_name}")
                
            except Exception as e:
                logger.error(f"Failed to collect data for {sql_file}: {str(e)}")
                # Return empty DataFrame for failed component
                component_name = sql_file.replace('.sql', '')
                results[component_name] = self._get_empty_dataframe_for_component(component_name)
        
        return results
    
    def _collect_with_fallback(self, connection, sql_file: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Collect data with fallback for PDCRINFO views that may not be available.
        
        Args:
            connection: Database connection object
            sql_file: SQL file name
            params: Parameters for placeholder replacement
        
        Returns:
            DataFrame containing the collected data
        
        Raises:
            Exception if the query fails
        """
        # Read SQL file
        sql = self.read_sql_file(sql_file)
        
        # Replace placeholders
        sql = self.replace_placeholders(sql, params)
        
        # Execute query
        df = self.execute_query(connection, sql)
        
        return df
    
    def _get_empty_dataframe_for_component(self, component_name: str) -> pd.DataFrame:
        """
        Return an empty DataFrame with expected columns for a component.
        
        Args:
            component_name: Name of the component
        
        Returns:
            Empty DataFrame with appropriate columns
        """
        column_mapping = {
            '01_full_table_scans': ['DatabaseName', 'TableName', 'TableKind', 'Size_GB', 'PeakSize_GB'],
            '02_highly_skewed_queries': ['DatabaseName', 'TableName', 'TableKind', 'Size_GB', 'PeakSize_GB', 'Recommendation'],
            '03_spool_usage_alerts': ['DatabaseName', 'TableName', 'TableKind', 'Size_GB', 'PeakSize_GB', 'Recommendation'],
            '04_unused_indexes': ['DatabaseName', 'TableName', 'IndexName', 'IndexType', 'IndexNumber', 'UniqueFlag', 'TableSize_GB', 'ColumnNames']
        }
        
        columns = column_mapping.get(component_name, [])
        return pd.DataFrame(columns=columns)
    
    def collect_single(self, connection, component_name: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Collect data for a single component.
        
        Args:
            connection: Database connection object
            component_name: Name of the component (e.g., '01_full_table_scans')
            params: Optional dictionary with parameters
        
        Returns:
            DataFrame containing the collected data
        """
        # Set default parameters
        if params is None:
            params = {}
        
        # Format system databases for SQL IN clause
        formatted_system_db_list = ", ".join([f"'{db}'" for db in SYSTEM_DATABASES])
        params['system_databases'] = formatted_system_db_list
        
        sql_file = f"{component_name}.sql"
        
        try:
            # Special handling for PDCRINFO views
            if component_name in ['01_full_table_scans', '02_highly_skewed_queries', '03_spool_usage_alerts']:
                try:
                    df = self._collect_with_fallback(connection, sql_file, params)
                    logger.info(f"Collected {len(df)} rows for {component_name}")
                    return df
                except Exception as e:
                    logger.warning(f"PDCRINFO view not available for {component_name}: {str(e)}")
                    return self._get_empty_dataframe_for_component(component_name)
            else:
                # Read SQL file
                sql = self.read_sql_file(sql_file)
                
                # Replace placeholders
                sql = self.replace_placeholders(sql, params)
                
                # Execute query
                df = self.execute_query(connection, sql)
                
                logger.info(f"Collected {len(df)} rows for {component_name}")
                return df
            
        except Exception as e:
            logger.error(f"Failed to collect data for {component_name}: {str(e)}")
            return self._get_empty_dataframe_for_component(component_name)
