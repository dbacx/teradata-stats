"""
Module 5 Config Collector

Collects configuration and logging data from Teradata system tables for the
Data Collection & Logging Config module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_collector import BaseCollector
from core.config import SYSTEM_DATABASES

# Configure logging
logger = logging.getLogger(__name__)


class ConfigCollector(BaseCollector):
    """
    Collector for Config Assessment Module (Module 4).
    
    Collects data from 4 components:
    1. ResUsage Rules - Validates ResUsage collection intervals
    2. DBQL Rules - Lists active DBQL logging rules
    3. DBQL Thresholds - Checks if thresholds are configured
    4. DBQL Tables Health - Checks health and size of DBQL logging tables
    """
    
    def __init__(self):
        """Initialize the Config Collector."""
        super().__init__(module_name='module_4_dbql')
        self.sql_files = [
            '01_resusage_rules.sql',
            '02_dbql_rules.sql',
            '03_dbql_thresholds.sql',
            '04_dbql_tables_health.sql'
        ]
        logger.info("Initialized ConfigCollector")
    
    def collect(self, connection, params: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Collect configuration data from Teradata.
        
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
                
                # Special handling for ResUsageRules which may not be available
                if component_name == '01_resusage_rules':
                    try:
                        df = self._collect_with_fallback(connection, sql_file, params)
                        results[component_name] = df
                        logger.info(f"Collected {len(df)} rows for {component_name}")
                    except Exception as e:
                        logger.warning(f"ResUsageRules view not available or permission denied: {str(e)}")
                        # Return empty DataFrame with expected columns
                        results[component_name] = pd.DataFrame(columns=['RuleName', 'RuleValue', 'RuleType'])
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
                results[component_name] = pd.DataFrame()
        
        return results
    
    def _collect_with_fallback(self, connection, sql_file: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Collect data with fallback for views that may not be available.
        
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
    
    def collect_single(self, connection, component_name: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Collect data for a single component.
        
        Args:
            connection: Database connection object
            component_name: Name of the component (e.g., '01_resusage_rules')
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
            # Special handling for ResUsageRules
            if component_name == '01_resusage_rules':
                try:
                    df = self._collect_with_fallback(connection, sql_file, params)
                    logger.info(f"Collected {len(df)} rows for {component_name}")
                    return df
                except Exception as e:
                    logger.warning(f"ResUsageRules view not available: {str(e)}")
                    return pd.DataFrame(columns=['RuleName', 'RuleValue', 'RuleType'])
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
            return pd.DataFrame()
