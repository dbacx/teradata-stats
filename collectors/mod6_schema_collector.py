"""
Module 7 Schema Collector

Collects schema design data from Teradata system tables for the
Schema Design & Best Practices module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_collector import BaseCollector
from core.config import SYSTEM_DATABASES

# Configure logging
logger = logging.getLogger(__name__)


class SchemaCollector(BaseCollector):
    """
    Collector for Schema Design Module (Module 6).
    
    Collects data from 3 components:
    1. Fallback Tables - Tables with Fallback protection enabled
    2. NoPI Tables - Tables without Primary Index
    3. Large CHAR Columns - Columns using fixed CHAR instead of VARCHAR
    """
    
    def __init__(self):
        """Initialize the Schema Collector."""
        super().__init__(module_name='module_6_schema')
        self.sql_files = [
            '01_fallback_tables.sql',
            '02_nopi_tables.sql',
            '03_large_char_columns.sql'
        ]
        logger.info("Initialized SchemaCollector")
    
    def collect(self, connection, params: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Collect schema design data from Teradata.
        
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
    
    def _get_empty_dataframe_for_component(self, component_name: str) -> pd.DataFrame:
        """
        Return empty DataFrame with expected columns for each component.
        
        Args:
            component_name: Name of the component
        
        Returns:
            Empty DataFrame with correct schema
        """
        if component_name == '01_fallback_tables':
            return pd.DataFrame(columns=[
                'DatabaseName', 'TableName', 'ProtectionType', 'TableKind',
                'CreateTimeStamp', 'LastAlterTimeStamp'
            ])
        elif component_name == '02_nopi_tables':
            return pd.DataFrame(columns=[
                'DatabaseName', 'TableName', 'TableKind', 'ProtectionType',
                'CreateTimeStamp'
            ])
        elif component_name == '03_large_char_columns':
            return pd.DataFrame(columns=[
                'DatabaseName', 'TableName', 'ColumnName', 'ColumnType',
                'ColumnLength', 'Nullable', 'DefaultValue'
            ])
        else:
            return pd.DataFrame()
