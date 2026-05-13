"""
Module 2 Statistics Collector

Collects data for the Statistics Management module by executing SQL queries
against Teradata system views (DBC.StatsV, DBC.TablesV, DBC.TableSizeV, etc.).
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from pathlib import Path
import sys
from core.base_collector import BaseCollector
from core.config import THRESHOLDS

# ---------------------------------------------------------
# BULLETPROOF PATH ROUTING
# ---------------------------------------------------------
# Sube exactamente 1 nivel desde collectors/ hasta teradata-stats/
current_file_path = Path(__file__).resolve()
project_root = str(current_file_path.parent.parent)

if project_root not in sys.path:
    sys.path.insert(0, project_root)  # insert(0) fuerza a Python a buscar aquí primero

logger = logging.getLogger(__name__)


class StatsCollector(BaseCollector):
    """
    Collector for Statistics Management Module (Module 2).
    
    Executes 10 SQL queries to identify statistics issues across the Teradata system:
    1. Unused Objects
    2. Sample Candidates
    3. Missing at PARTITION level
    4. Missing at Table Level
    5. Missing at Index Level
    6. Stale Stats
    7. Zero Stats
    8. Multicolumn
    9. Skipped and Sample
    10. DBC Recommendations
    """
    
    def __init__(self):
        """Initialize the Stats Collector."""
        super().__init__(module_name='module_2_stats')
        self.sql_files = [
            '01_unused_objects.sql',
            '02_sample_candidates.sql',
            '03_missing_partition.sql',
            '04_missing_table.sql',
            '05_missing_index.sql',
            '06_stale_stats.sql',
            '07_zero_stats.sql',
            '08_multicolumn.sql',
            '09_skipped_sample.sql',
            '10_dbc_recommendations.sql'
        ]
        logger.info(f"Initialized StatsCollector with {len(self.sql_files)} SQL files")
    
    def collect(self, connection, params: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Collect data from all 10 SQL components.
        
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
                'stale_days_threshold': THRESHOLDS['stats_stale_days'],
                'max_value_length_threshold': 25
            }
        
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
            component_name: Name of the component (e.g., '01_unused_objects')
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
                'stale_days_threshold': THRESHOLDS['stats_stale_days'],
                'max_value_length_threshold': 25
            }
        
        logger.info(f"Collecting data for single component: {component_name}")
        
        # Read SQL file
        sql = self.read_sql_file(sql_file)
        
        # Replace placeholders
        sql = self.replace_placeholders(sql, params)
        
        # Execute query
        df = self.execute_query(connection, sql)
        
        logger.info(f"Successfully collected {len(df)} rows for {component_name}")
        return df
