"""
Base Collector Abstract Class for Data Collection

This module defines the abstract base class that all data collectors
must inherit from, providing a consistent interface for SQL-based data extraction.
"""

import logging
import pandas as pd
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """
    Abstract base class for data collectors.
    
    All specific collector implementations must inherit from this class and
    implement the collect method. This ensures consistency across all
    collectors and enables SQL-based data extraction with placeholder replacement.
    """
    
    def __init__(self, module_name: str):
        """
        Initialize the base collector with module metadata.
        
        Args:
            module_name: Name of the module (e.g., "mod2_stats")
        """
        self.module_name = module_name
        self.collection_date = datetime.now()
        self.sql_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sql', module_name)
        
        logger.debug(f"Initialized collector for module: {self.module_name}")
    
    @abstractmethod
    def collect(self, connection, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Collect data from the database using SQL queries.
        
        This method must be implemented by each concrete collector class.
        It should read SQL files, replace placeholders, and execute queries.
        
        Args:
            connection: Database connection object
            params: Optional dictionary of parameters for placeholder replacement
                    (e.g., {'date_from': '2024-01-01', 'date_to': '2024-12-31'})
        
        Returns:
            DataFrame containing the collected data
        
        Raises:
            FileNotFoundError: If SQL file is not found
            Exception: If query execution fails
        """
        pass
    
    def read_sql_file(self, sql_filename: str) -> str:
        """
        Read SQL file from the module's SQL directory.
        
        Args:
            sql_filename: Name of the SQL file (e.g., "2_1_unused_objects.sql")
        
        Returns:
            String containing the SQL query
        
        Raises:
            FileNotFoundError: If SQL file is not found
        """
        sql_path = os.path.join(self.sql_dir, sql_filename)
        
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        logger.debug(f"Read SQL file: {sql_filename}")
        return sql_content
    
    def replace_placeholders(self, sql: str, params: Optional[Dict[str, Any]] = None) -> str:
        """
        Replace placeholders in SQL query with actual values.
        
        Args:
            sql: SQL query string with placeholders (e.g., {date_from})
            params: Dictionary of parameter values
        
        Returns:
            SQL query with placeholders replaced
        """
        if params:
            try:
                sql = sql.format(**params)
                logger.debug(f"Replaced placeholders: {list(params.keys())}")
            except KeyError as e:
                logger.error(f"Missing placeholder parameter: {e}")
                raise ValueError(f"Missing placeholder parameter: {e}")
        
        return sql
    
    def execute_query(self, connection, sql: str) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.
        
        Args:
            connection: Database connection object
            sql: SQL query string
        
        Returns:
            DataFrame containing query results
        
        Raises:
            Exception: If query execution fails
        """
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            
            cursor.close()
            
            logger.info(f"Query executed successfully. Rows returned: {len(df)}")
            return df
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def get_module_info(self) -> Dict[str, Any]:
        """
        Get information about this collector.
        
        Returns:
            Dictionary containing collector metadata
        """
        return {
            'module_name': self.module_name,
            'collection_date': self.collection_date,
            'sql_dir': self.sql_dir
        }
    
    def __str__(self) -> str:
        """String representation of the collector."""
        return f"BaseCollector(module='{self.module_name}')"
    
    def __repr__(self) -> str:
        """Detailed string representation of the collector."""
        return f"BaseCollector(module_name='{self.module_name}', collection_date='{self.collection_date}')"
