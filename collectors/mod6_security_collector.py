"""
Module 6 Security Collector

Collects security-related data from Teradata system tables for the
User & Security Management module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_collector import BaseCollector
from core.config import SYSTEM_DATABASES

# Configure logging
logger = logging.getLogger(__name__)


class SecurityCollector(BaseCollector):
    """
    Collector for Security Assessment Module (Module 6).
    
    Collects data from 5 components:
    1. Password Expiry - Users with expired passwords
    2. Users Without Profile - Users without assigned profiles
    3. Direct Grants - Users with direct access rights instead of roles
    4. Stagnant Users - Users with no recent access
    5. Users Without Role - Productive users not in any role
    """
    
    def __init__(self):
        """Initialize the Security Collector."""
        super().__init__(module_name='module_6_security')
        self.sql_files = [
            '01_password_expiry.sql',
            '02_users_without_profile.sql',
            '03_direct_grants.sql',
            '04_stagnant_users.sql',
            '05_users_without_role.sql'
        ]
        logger.info("Initialized SecurityCollector")
    
    def collect(self, connection, params: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Collect security data from Teradata.
        
        Args:
            connection: Database connection object
            params: Optional dictionary with thresholds:
                    - password_expiry_days_threshold (default: 90)
                    - unused_days_threshold (default: 30)
        
        Returns:
            Dictionary mapping component names to DataFrames
        """
        # Set default parameters
        if params is None:
            params = {}
        
        params.setdefault('password_expiry_days_threshold', 90)
        params.setdefault('unused_days_threshold', 30)
        
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
                results[component_name] = pd.DataFrame()
        
        return results
    
    def collect_single(self, connection, component_name: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Collect data for a single component.
        
        Args:
            connection: Database connection object
            component_name: Name of the component (e.g., '01_password_expiry')
            params: Optional dictionary with thresholds
        
        Returns:
            DataFrame containing the collected data
        """
        # Set default parameters
        if params is None:
            params = {}
        
        params.setdefault('password_expiry_days_threshold', 90)
        params.setdefault('unused_days_threshold', 30)
        
        # Format system databases for SQL IN clause
        formatted_system_db_list = ", ".join([f"'{db}'" for db in SYSTEM_DATABASES])
        params['system_databases'] = formatted_system_db_list
        
        sql_file = f"{component_name}.sql"
        
        try:
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
