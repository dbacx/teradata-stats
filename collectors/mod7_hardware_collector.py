"""
Module 8 Hardware Collector

Collects hardware utilization data from Teradata system tables for the
Hardware Utilization module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_collector import BaseCollector

# Configure logging
logger = logging.getLogger(__name__)


class HardwareCollector(BaseCollector):
    """
    Collector for Hardware Utilization Module (Module 7).
    
    Collects data from 2 components:
    1. AMP Space Skew - Space distribution across AMPs
    2. Node CPU Usage - CPU usage by node for current day
    """
    
    def __init__(self):
        """Initialize the Hardware Collector."""
        super().__init__(module_name='module_7_hardware')
        self.sql_files = [
            '01_amp_space_skew.sql',
            '02_node_cpu.sql'
        ]
        logger.info("Initialized HardwareCollector")
    
    def collect(self, connection, params: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Collect hardware utilization data from Teradata.
        
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
                
                # Special handling for ResUsageSpma which may be disabled
                if component_name == '02_node_cpu':
                    try:
                        # Read SQL file
                        sql = self.read_sql_file(sql_file)
                        
                        # Replace placeholders
                        sql = self.replace_placeholders(sql, params)
                        
                        # Execute query
                        df = self.execute_query(connection, sql)
                        
                        results[component_name] = df
                        logger.info(f"Collected {len(df)} rows for {component_name}")
                    except Exception as e:
                        logger.warning(f"ResUsageSpma logging may be disabled or permission denied for {component_name}: {str(e)}")
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
    
    def _get_empty_dataframe_for_component(self, component_name: str) -> pd.DataFrame:
        """
        Return empty DataFrame with expected columns for each component.
        
        Args:
            component_name: Name of the component
        
        Returns:
            Empty DataFrame with correct schema
        """
        if component_name == '01_amp_space_skew':
            return pd.DataFrame(columns=['AMP_ID', 'TotalSpace_Bytes'])
        elif component_name == '02_node_cpu':
            return pd.DataFrame(columns=['TheDate', 'NodeID', 'TotalIdle', 'TotalServ'])
        else:
            return pd.DataFrame()
