"""
Module 1 Health Collector

Collects health and connectivity data from Teradata system tables for the
Health & Connectivity module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_collector import BaseCollector

# Configure logging
logger = logging.getLogger(__name__)


class HealthCollector(BaseCollector):
    """
    Collector for Health & Connectivity Module (Module 1).
    
    Collects data from 2 components:
    1. System Information - Version and release information
    2. Active Sessions - Count of sessions by user
    """
    
    def __init__(self):
        """Initialize the Health Collector."""
        super().__init__(module_name='module_1_health')
        self.sql_files = [
            '01_system_info.sql',
            '02_active_sessions.sql'
        ]
        logger.info("Initialized HealthCollector")
    
    def collect(self, connection, params: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Collect health and connectivity data from Teradata.
        
        Args:
            connection: Database connection object
            params: Optional dictionary with parameters (not used in this module)
        
        Returns:
            Dictionary mapping component names to DataFrames
        """
        # Set default parameters
        if params is None:
            params = {}
        
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
        if component_name == '01_system_info':
            return pd.DataFrame(columns=['InfoKey', 'InfoData'])
        elif component_name == '02_active_sessions':
            return pd.DataFrame(columns=['UserName', 'SessionCount'])
        else:
            return pd.DataFrame()
