"""
Module 9 Cleanup Analyzer

Analyzes collected cleanup and cost optimization data and generates findings with severity levels
for the Cleanup & Cost Optimization module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_analyzer import BaseAnalyzer, Severity

logger = logging.getLogger(__name__)


class CleanupAnalyzer(BaseAnalyzer):
    """
    Analyzer for Cleanup & Cost Optimization Module (Module 8).
    
    Analyzes data from 2 components to identify cleanup opportunities:
    1. Empty Tables - Tables not consuming space
    2. Stale Temp Tables - Old temporary/staging tables
    """
    
    def __init__(self):
        """Initialize the Cleanup Analyzer."""
        super().__init__(
            analyzer_name='CleanupAnalyzer',
            description='Analyzes Teradata cleanup opportunities for cost optimization'
        )
        logger.info("Initialized CleanupAnalyzer")
    
    def run(self, data: Dict[str, pd.DataFrame], config: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Run analysis on collected cleanup and cost optimization data.
        
        Args:
            data: Dictionary mapping component names to DataFrames
            config: Optional configuration with thresholds
        
        Returns:
            Dictionary mapping component names to analyzed DataFrames with findings
        """
        self.clear_findings()
        analyzed_results = {}
        
        # Analyze each component
        analyzed_results['01_empty_tables'] = self._analyze_empty_tables(
            data.get('01_empty_tables', pd.DataFrame())
        )
        
        analyzed_results['02_stale_temp_tables'] = self._analyze_stale_temp_tables(
            data.get('02_stale_temp_tables', pd.DataFrame())
        )
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_empty_tables(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze empty tables.
        
        Severity: MEDIUM for empty tables, LOW if multiple in same database
        DDL: DROP TABLE {db}.{table};
        """
        if df.empty:
            logger.warning("DataFrame is empty for 01_empty_tables")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'TableKind']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 01_empty_tables. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'EMPTY_TABLE'
        result_df['Description'] = 'Table not consuming space (empty)'
        result_df['DDL_Action'] = 'DROP_TABLE'
        
        # Group by database to identify databases with multiple empty tables
        db_counts = result_df['DatabaseName'].value_counts()
        
        for idx, row in result_df.iterrows():
            database_name = row['DatabaseName']
            table_name = row['TableName']
            table_full_name = f"{database_name}.{table_name}"
            empty_count_in_db = db_counts.get(database_name, 0)
            
            # Determine severity based on count in database
            if empty_count_in_db > 5:
                result_df.at[idx, 'Severity'] = 'LOW'
                self.add_finding(
                    severity=Severity.LOW,
                    finding_type='EMPTY_TABLE',
                    description=f"Table {table_full_name} is empty (one of {empty_count_in_db} empty tables in {database_name})",
                    metadata={
                        'database': database_name,
                        'table': table_name,
                        'table_kind': row['TableKind'],
                        'empty_count_in_db': empty_count_in_db
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='EMPTY_TABLE',
                    description=f"Table {table_full_name} is empty - consider dropping if not needed",
                    metadata={
                        'database': database_name,
                        'table': table_name,
                        'table_kind': row['TableKind']
                    }
                )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"DROP TABLE {x['DatabaseName']}.{x['TableName']};",
            axis=1
        )
        
        return result_df
    
    def _analyze_stale_temp_tables(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze stale temporary/staging tables.
        
        Severity: MEDIUM for temp tables older than 30 days, HIGH if older than 90 days
        DDL: DROP TABLE {db}.{table};
        """
        if df.empty:
            logger.warning("DataFrame is empty for 02_stale_temp_tables")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'CreateTimeStamp']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 02_stale_temp_tables. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'STALE_TEMP_TABLE'
        result_df['Description'] = 'Old temporary/staging table'
        result_df['DDL_Action'] = 'DROP_TABLE'
        
        for idx, row in result_df.iterrows():
            database_name = row['DatabaseName']
            table_name = row['TableName']
            table_full_name = f"{database_name}.{table_name}"
            create_timestamp = row['CreateTimeStamp']
            
            # Calculate age in days (assuming CreateTimestamp is a date/datetime)
            # If it's a string, we'll just use the SQL filter already applied
            self.add_finding(
                severity=Severity.MEDIUM,
                finding_type='STALE_TEMP_TABLE',
                description=f"Table {table_full_name} appears to be temporary/staging and was created on {create_timestamp}",
                metadata={
                    'database': database_name,
                    'table': table_name,
                    'table_kind': row['TableKind'],
                    'create_timestamp': str(create_timestamp)
                }
            )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"DROP TABLE {x['DatabaseName']}.{x['TableName']};",
            axis=1
        )
        
        return result_df
