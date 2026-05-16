"""
Module 10 Monthly Report Collector

Collects data for the Monthly Report module by executing SQL queries
against Teradata system views with dynamic date parameterization.
"""

import logging
import pandas as pd
import re
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional

# ---------------------------------------------------------
# BULLETPROOF PATH ROUTING
# ---------------------------------------------------------
# Sube exactamente 1 nivel desde collectors/mod10_monthly_collector.py hasta teradata-stats/
current_file_path = Path(__file__).resolve()
project_root = str(current_file_path.parent.parent)

if project_root not in sys.path:
    sys.path.insert(0, project_root)  # insert(0) fuerza a Python a buscar aquí primero

from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class MonthlyReportCollector(BaseCollector):
    """
    Collector for Monthly Report Module (Module 9).
    
    Executes 69 SQL queries for monthly reporting with dynamic date parameterization.
    Supports single report execution and batch execution for all reports.
    """
    
    def __init__(self):
        """Initialize the Monthly Report Collector."""
        super().__init__(module_name='module_9_monthly_report')
        self.sql_dir = Path("sql/module_9_monthly_report")
        self.sql_files = sorted(self.sql_dir.glob("*.sql"))
        logger.info(f"Initialized MonthlyReportCollector with {len(self.sql_files)} SQL files")
    
    def get_report_list(self) -> list[str]:
        """
        Get list of available report names.
        
        Returns:
            List of report filenames
        """
        return [f.name for f in self.sql_files]
    
    def collect(self, connection, *args, **kwargs):
        """
        Implementación obligatoria del método abstracto de BaseCollector.
        Enruta la ejecución hacia collect_single o collect_all según los argumentos.
        
        Args:
            connection: Teradata connection object
            *args: Variable positional arguments
            **kwargs: Keyword arguments including:
                - report_name: Name of specific report to execute
                - start_date: Start date in YYYY-MM-DD format
                - end_date: End date in YYYY-MM-DD format
        
        Returns:
            DataFrame (if single report) or Dict[str, DataFrame] (if all reports)
        """
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        
        # Si la UI pasa un report_name específico, ejecuta el single
        if 'report_name' in kwargs and kwargs['report_name'] and kwargs['report_name'] != 'Todos':
            return self.collect_single(connection, kwargs['report_name'], start_date, end_date)
        
        # De lo contrario, o si se pide ejecución batch, ejecuta all
        return self.collect_all(connection, start_date, end_date)
    
    def replace_dates_in_sql(self, sql: str, start_date: str, end_date: str) -> str:
        """
        Replace hardcoded dates in SQL with dynamic dates using regex.
        
        Args:
            sql: SQL query string
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        
        Returns:
            SQL with dates replaced
        """
        # Pattern for dates in format 'YYYY-MM-DD' or DATE'YYYY-MM-DD'
        date_pattern = r"(?:DATE\s*)?'(\d{4}-\d{2}-\d{2})'"
        
        def replace_date(match):
            """Replace a single date match with the appropriate parameter."""
            # Determine if this is a start or end date based on context
            # For simplicity, we'll replace the first occurrence with start_date
            # and subsequent occurrences with end_date
            nonlocal replace_count
            replace_count += 1
            if replace_count == 1:
                return f"DATE'{start_date}'"
            else:
                return f"DATE'{end_date}'"
        
        replace_count = 0
        sql_with_dates = re.sub(date_pattern, replace_date, sql, flags=re.IGNORECASE)
        
        return sql_with_dates
    
    def collect_single(self, connection, report_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Collect data from a single report with date parameterization.
        
        Args:
            connection: Teradata connection object (teradatasql.connect)
            report_name: Name of the report file (e.g., 'SqlTextInfo001.sql')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        
        Returns:
            DataFrame containing the collected data
        
        Raises:
            FileNotFoundError: If report SQL file is not found
        """
        logger.info(f"Collecting data for single report: {report_name} from {start_date} to {end_date}")
        
        # Read SQL file
        sql_file = self.sql_dir / report_name
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        # Replace dates dynamically
        sql = self.replace_dates_in_sql(sql, start_date, end_date)
        
        # Execute query using base collector method
        df = self.execute_query(connection, sql)
        
        logger.info(f"Successfully collected {len(df)} rows for {report_name}")
        return df
    
    def collect_all(self, connection, start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """
        Collect data from all 69 reports with date parameterization in alphanumeric order.
        
        Args:
            connection: Teradata connection object (teradatasql.connect)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        
        Returns:
            Dictionary mapping report names (without .sql extension) to DataFrames
        """
        results = {}
        
        # Sort SQL files alphanumerically
        sorted_sql_files = sorted(self.sql_files)
        
        for sql_file in sorted_sql_files:
            report_name_with_ext = sql_file.name
            report_name = report_name_with_ext.replace('.sql', '')
            
            try:
                logger.info(f"Collecting data for report: {report_name}")
                
                # Read SQL file
                with open(sql_file, 'r', encoding='utf-8') as f:
                    sql = f.read()
                
                # Replace dates dynamically
                sql = self.replace_dates_in_sql(sql, start_date, end_date)
                
                # Execute query using base collector method
                df = self.execute_query(connection, sql)
                
                results[report_name] = df
                logger.info(f"Successfully collected {len(df)} rows for {report_name}")
                
            except Exception as e:
                logger.error(f"Failed to collect data for {report_name}: {str(e)}")
                # Return empty DataFrame on failure to allow other reports to continue
                results[report_name] = pd.DataFrame()
        
        logger.info(f"Collected data from {len(results)} reports")
        return results
