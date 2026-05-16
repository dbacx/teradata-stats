"""
Module 4 Space Collector

Collects data for the Space Assessment module by executing SQL queries
against Teradata system views (DBC.DiskSpaceV, DBC.TableSizeV, DBC.ColumnsV, etc.).
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_collector import BaseCollector
from core.config import THRESHOLDS, SYSTEM_DATABASES

logger = logging.getLogger(__name__)


class SpaceCollector(BaseCollector):
    """
    Collector for Space Assessment Module (Module 3).
    
    Executes 5 SQL queries to analyze space usage across the Teradata system:
    1. Database Space Utilization
    2. Unused Tables Space
    3. MVC (Multi-Value Compression) Candidates
    4. Top Tables by Size
    5. Skewed Tables
    """
    
    def __init__(self):
        """Initialize the Space Collector."""
        super().__init__(module_name='module_3_space')
        self.sql_files = [
            '01_db_space_utilization.sql',
            '02_unused_tables_space.sql',
            '03_mvc_candidates.sql',
            '04_top_tables.sql',
            '05_skewed_tables.sql'
        ]
        logger.info(f"Initialized SpaceCollector with {len(self.sql_files)} SQL files")
    
    def collect(self, connection, params: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Collect data from all 5 SQL components.
        
        Args:
            connection: Teradata connection object
            params: Optional dictionary of parameters for placeholder replacement.
                    Defaults to using THRESHOLDS from config if not provided.
        
        Returns:
            Dictionary mapping component names to DataFrames
        """
        # Use config thresholds if no params provided
        if params is None:
            params = {
                'unused_days_threshold': THRESHOLDS['unused_object_days'],
                'skew_pct_threshold': THRESHOLDS['pi_skew_pct']
            }
        
        # Format system databases for SQL IN clause
        system_databases_str = ", ".join([f"'{db}'" for db in SYSTEM_DATABASES])
        params['system_databases'] = system_databases_str
        
        results = {}
        
        for sql_file in self.sql_files:
            component_name = sql_file.replace('.sql', '')
            
            try:
                logger.info(f"Collecting data for component: {component_name}")
                
                # Read SQL file
                sql = self.read_sql_file(sql_file)
                
                # Replace placeholders
                sql = self.replace_placeholders(sql, params)
                
                # Execute query
                df = self.execute_query(connection, sql)
                
                results[component_name] = df
                logger.info(f"Successfully collected {len(df)} rows for {component_name}")
                
            except Exception as e:
                logger.error(f"Failed to collect data for {component_name}: {str(e)}")
                # Return empty DataFrame on failure to allow other components to continue
                results[component_name] = pd.DataFrame()
        
        logger.info(f"Collected data from {len(results)} components")
        return results
    
    def collect_single(self, connection, component_name: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Collect data from a single component.
        
        Args:
            connection: Teradata connection object
            component_name: Name of the component (e.g., '01_db_space_utilization')
            params: Optional dictionary of parameters for placeholder replacement
        
        Returns:
            DataFrame containing the collected data
        
        Raises:
            FileNotFoundError: If component SQL file is not found
        """
        sql_file = f"{component_name}.sql"
        
        # Use config thresholds if no params provided
        if params is None:
            params = {
                'unused_days_threshold': THRESHOLDS['unused_object_days'],
                'skew_pct_threshold': THRESHOLDS['pi_skew_pct']
            }
        
        # Format system databases for SQL IN clause
        system_databases_str = ", ".join([f"'{db}'" for db in SYSTEM_DATABASES])
        params['system_databases'] = system_databases_str
        
        logger.info(f"Collecting data for single component: {component_name}")
        
        # Read SQL file
        sql = self.read_sql_file(sql_file)
        
        # Replace placeholders
        sql = self.replace_placeholders(sql, params)
        
        # Execute query
        df = self.execute_query(connection, sql)
        
        logger.info(f"Successfully collected {len(df)} rows for {component_name}")
        return df
