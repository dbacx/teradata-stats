"""
Module 1 System Information Collector

Collects system information data from Teradata system tables for
System Information module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_collector import BaseCollector

# Configure logging
logger = logging.getLogger(__name__)


class SystemInformationCollector(BaseCollector):
    """
    Collector for System Information Module (Module 1).
    
    Collects data from a single component:
    1. System Information - Version, release, and capacity information
    """
    
    def __init__(self):
        """Initialize System Information Collector."""
        super().__init__(module_name='module_1_health')
        self.sql_file = '01_system_information.sql'
        logger.info("Initialized SystemInformationCollector")
    
    def collect(self, connection, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Collect system information data from Teradata.
        
        Args:
            connection: Database connection object
            params: Optional dictionary with parameters (not used in this module)
        
        Returns:
            DataFrame containing system information with Metrica and Valor columns
        """
        # Set default parameters
        if params is None:
            params = {}
        
        try:
            # Read SQL file
            sql = self.read_sql_file(self.sql_file)
            
            # Replace placeholders
            sql = self.replace_placeholders(sql, params)
            
            # Split SQL content by semicolon and process each query
            sql_statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
            
            all_results = []
            
            for i, statement in enumerate(sql_statements):
                try:
                    if 'exec pdcrinfo.system_config_rpt' in statement.lower():
                        # Process hardware macro query
                        df_hardware = self._process_hardware_macro(connection, statement)
                        all_results.append(df_hardware)
                    elif 'select' in statement.lower() and 'dbc.dbcinfo' in statement.lower():
                        # Process version query
                        df_version = self._process_version_query(connection, statement)
                        all_results.append(df_version)
                    else:
                        logger.warning(f"Unknown statement type in query {i+1}: {statement[:50]}...")
                        
                except Exception as e:
                    logger.error(f"Failed to execute statement {i+1}: {str(e)}")
                    # Add error row for failed statement
                    error_row = pd.DataFrame([{
                        'Metrica': f'Query {i+1} Error',
                        'Valor': f'Error: {str(e)[:100]}'
                    }])
                    all_results.append(error_row)
            
            # Consolidate all results
            if all_results:
                consolidated_df = pd.concat(all_results, ignore_index=True)
                logger.info(f"Collected {len(consolidated_df)} rows for system information")
                return consolidated_df
            else:
                logger.warning("No data collected from any query")
                return pd.DataFrame(columns=['Metrica', 'Valor'])
            
        except Exception as e:
            logger.error(f"Failed to collect system information: {str(e)}")
            # Return empty DataFrame on failure
            return pd.DataFrame(columns=['Metrica', 'Valor'])
    
    def _process_hardware_macro(self, connection, statement: str) -> pd.DataFrame:
        """
        Process hardware macro query and convert to vertical format.
        
        Args:
            connection: Database connection object
            statement: SQL statement containing the hardware macro
        
        Returns:
            DataFrame with Metrica and Valor columns
        """
        try:
            # Execute hardware macro query
            df_hardware = self.execute_query(connection, statement)
            
            if df_hardware.empty:
                logger.warning("Hardware macro returned empty results")
                return pd.DataFrame([{
                    'Metrica': 'Hardware Config (PDCR)',
                    'Valor': 'No data returned from hardware macro'
                }])
            
            # Convert wide DataFrame to vertical format using melt
            df_vertical = df_hardware.melt(var_name='Metrica', value_name='Valor')
            
            # Ensure Valor column is string type to avoid type conflicts
            df_vertical['Valor'] = df_vertical['Valor'].astype(str)
            
            logger.info(f"Processed hardware macro: {len(df_vertical)} metrics extracted")
            return df_vertical
            
        except Exception as e:
            logger.error(f"Failed to process hardware macro: {str(e)}")
            # Return error row for PDCR permissions issue
            return pd.DataFrame([{
                'Metrica': 'Hardware Config (PDCR)',
                'Valor': 'Error de permisos o PDCR no habilitado'
            }])
    
    def _process_version_query(self, connection, statement: str) -> pd.DataFrame:
        """
        Process version query from DBC.DBCInfo.
        
        Args:
            connection: Database connection object
            statement: SQL statement for version query
        
        Returns:
            DataFrame with Metrica and Valor columns
        """
        try:
            # Execute version query
            df_version = self.execute_query(connection, statement)
            
            if df_version.empty:
                logger.warning("Version query returned empty results")
                return pd.DataFrame([{
                    'Metrica': 'Database Version',
                    'Valor': 'Version information not available'
                }])
            
            # Ensure Valor column is string type
            df_version['Valor'] = df_version['Valor'].astype(str)
            
            logger.info(f"Processed version query: {len(df_version)} version records")
            return df_version
            
        except Exception as e:
            logger.error(f"Failed to process version query: {str(e)}")
            return pd.DataFrame([{
                'Metrica': 'Database Version',
                'Valor': f'Error retrieving version: {str(e)[:50]}'
            }])
