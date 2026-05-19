"""
Module 3 Space Collector

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
    
    Executes 11 SQL queries to analyze space usage across the Teradata system:
     1. CDS Report (Customer Data Space)
     2. Space Capacity Forecast (Historical Baseline)
     3. Suspected Unused Objects
     4. Suspected Duplicate Objects
     5. MVC Opportunities - Uncompressed Tables
     6. MVC Opportunities - Compressed Tables
     7. Top 20 Databases By Used Size
     8. Top 20 Tables By Size
     9. Top 20 Unused Databases By Size
    10. Monthly Capacity Snapshot
    11. Database Space Utilization (AMP-Aware)
    """
    
    def __init__(self):
        """Initialize the Space Collector."""
        super().__init__(module_name='module_3_space')
        self.sql_files = [
            '01_CDS_Report.sql',
            '02_Space_Capacity_Forecast.sql',
            '03_Suspected_Unused_Objects.sql',
            '04_Suspected_Duplicate_Objects.sql',
            '05_MVC_Opportunities_Uncompressed_Tables.sql',
            '06_MVC_Opportunities_Compressed_Tables.sql',
            '07_Top_20_Databases_By_Used_Size.sql',
            '08_Top_20_Tables_By_Size.sql',
            '09_Top_20_Unused_Databases_By_Size.sql',
            '10_Monthly_Capacity_ Snapshot.sql',
            '11_Database_Space_Utilization.sql',
        ]
        logger.info(f"Initialized SpaceCollector with {len(self.sql_files)} SQL files")
    
    def collect(self, connection, params: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Collect data from all 11 SQL components.
        
        Args:
            connection: Teradata connection object
            params: Optional dictionary of parameters for placeholder replacement.
                    Defaults to using THRESHOLDS from config if not provided.
        
        Returns:
            Dictionary mapping component names to DataFrames
        """
        if params is None:
            params = {
                'unused_days_threshold': THRESHOLDS['unused_object_days'],
                'skew_pct_threshold': THRESHOLDS['pi_skew_pct']
            }
        
        system_databases_str = ", ".join([f"'{db}'" for db in SYSTEM_DATABASES])
        params['system_databases'] = system_databases_str
        
        results = {}
        
        for sql_file in self.sql_files:
            component_name = sql_file.replace('.sql', '')
            
            try:
                logger.info(f"Collecting data for component: {component_name}")
                
                sql = self.read_sql_file(sql_file)
                sql = self.replace_placeholders(sql, params)
                df = self.execute_query(connection, sql)
                
                results[component_name] = df
                logger.info(f"Successfully collected {len(df)} rows for {component_name}")
                
            except Exception as e:
                logger.error(f"Failed to collect data for {component_name}: {str(e)}")
                results[component_name] = pd.DataFrame()
        
        logger.info(f"Collected data from {len(results)} components")
        return results
    
    def collect_single(self, connection, component_name: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Collect data from a single component.
        
        Args:
            connection: Teradata connection object
            component_name: Name of the component (e.g., '01_CDS_Report')
            params: Optional dictionary of parameters for placeholder replacement
        
        Returns:
            DataFrame containing the collected data
        
        Raises:
            FileNotFoundError: If component SQL file is not found
        """
        sql_file = f"{component_name}.sql"
        
        if params is None:
            params = {
                'unused_days_threshold': THRESHOLDS['unused_object_days'],
                'skew_pct_threshold': THRESHOLDS['pi_skew_pct']
            }
        
        system_databases_str = ", ".join([f"'{db}'" for db in SYSTEM_DATABASES])
        params['system_databases'] = system_databases_str
        
        logger.info(f"Collecting data for single component: {component_name}")
        
        sql = self.read_sql_file(sql_file)
        sql = self.replace_placeholders(sql, params)
        df = self.execute_query(connection, sql)
        
        logger.info(f"Successfully collected {len(df)} rows for {component_name}")
        return df
