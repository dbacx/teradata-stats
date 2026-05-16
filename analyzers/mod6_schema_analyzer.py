"""
Module 7 Schema Analyzer

Analyzes collected schema design data and generates findings with severity levels
and DDL remediation statements for the Schema Design & Best Practices module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_analyzer import BaseAnalyzer, Severity

logger = logging.getLogger(__name__)


class SchemaAnalyzer(BaseAnalyzer):
    """
    Analyzer for Schema Design Module (Module 6).
    
    Analyzes data from 3 components to identify schema design issues:
    1. Fallback Tables - Tables with Fallback protection enabled
    2. NoPI Tables - Tables without Primary Index
    3. Large CHAR Columns - Columns using fixed CHAR instead of VARCHAR
    """
    
    def __init__(self):
        """Initialize the Schema Analyzer."""
        super().__init__(
            analyzer_name='SchemaAnalyzer',
            description='Analyzes Teradata schema design for best practices compliance'
        )
        logger.info("Initialized SchemaAnalyzer")
    
    def run(self, data: Dict[str, pd.DataFrame], config: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Run analysis on collected schema design data.
        
        Args:
            data: Dictionary mapping component names to DataFrames
            config: Optional configuration with thresholds
        
        Returns:
            Dictionary mapping component names to analyzed DataFrames with findings
        """
        self.clear_findings()
        analyzed_results = {}
        
        # Analyze each component
        analyzed_results['01_fallback_tables'] = self._analyze_fallback_tables(
            data.get('01_fallback_tables', pd.DataFrame())
        )
        
        analyzed_results['02_nopi_tables'] = self._analyze_nopi_tables(
            data.get('02_nopi_tables', pd.DataFrame())
        )
        
        analyzed_results['03_large_char_columns'] = self._analyze_large_char_columns(
            data.get('03_large_char_columns', pd.DataFrame())
        )
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_fallback_tables(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze tables with Fallback protection enabled.
        
        Severity: MEDIUM for tables with Fallback (duplicates space)
        DDL: ALTER TABLE ... NO FALLBACK
        """
        if df.empty:
            logger.warning("DataFrame is empty for 01_fallback_tables")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'ProtectionType']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 01_fallback_tables. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'FALLBACK_ENABLED'
        result_df['Description'] = 'Table has Fallback protection enabled (duplicates space)'
        result_df['DDL_Action'] = 'REMOVE_FALLBACK'
        
        for idx, row in result_df.iterrows():
            table_name = f"{row['DatabaseName']}.{row['TableName']}"
            
            self.add_finding(
                severity=Severity.MEDIUM,
                finding_type='FALLBACK_ENABLED',
                description=f"Table {table_name} has Fallback protection enabled - consider removing if not required",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'protection_type': row['ProtectionType']
                }
            )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"ALTER TABLE {x['DatabaseName']}.{x['TableName']} NO FALLBACK;",
            axis=1
        )
        
        return result_df
    
    def _analyze_nopi_tables(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze tables without Primary Index.
        
        Severity: HIGH for tables without PI in transactional environment
        DDL: Review table design - consider adding PI
        """
        if df.empty:
            logger.warning("DataFrame is empty for 02_nopi_tables")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'TableKind']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 02_nopi_tables. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'HIGH'
        result_df['Finding_Type'] = 'NO_PRIMARY_INDEX'
        result_df['Description'] = 'Table without Primary Index'
        result_df['DDL_Action'] = 'REVIEW_DESIGN'
        
        for idx, row in result_df.iterrows():
            table_name = f"{row['DatabaseName']}.{row['TableName']}"
            
            self.add_finding(
                severity=Severity.HIGH,
                finding_type='NO_PRIMARY_INDEX',
                description=f"Table {table_name} has no Primary Index - review for transactional workloads",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'table_kind': row['TableKind']
                }
            )
        
        # Generate DDL (suggestions)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- Review table design for {x['DatabaseName']}.{x['TableName']} - consider adding Primary Index",
            axis=1
        )
        
        return result_df
    
    def _analyze_large_char_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze columns using fixed CHAR instead of VARCHAR for large sizes.
        
        Severity: LOW for CHAR columns > 100 bytes (space waste)
        DDL: ALTER TABLE ... MODIFY COLUMN ... VARCHAR
        """
        if df.empty:
            logger.warning("DataFrame is empty for 03_large_char_columns")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'ColumnName', 'ColumnLength']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 03_large_char_columns. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'LOW'
        result_df['Finding_Type'] = 'LARGE_CHAR_COLUMN'
        result_df['Description'] = 'Column uses fixed CHAR instead of VARCHAR'
        result_df['DDL_Action'] = 'CONVERT_TO_VARCHAR'
        
        for idx, row in result_df.iterrows():
            column_name = f"{row['DatabaseName']}.{row['TableName']}.{row['ColumnName']}"
            column_length = row['ColumnLength']
            
            self.add_finding(
                severity=Severity.LOW,
                finding_type='LARGE_CHAR_COLUMN',
                description=f"Column {column_name} uses CHAR({column_length}) - consider VARCHAR to save space",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'column': row['ColumnName'],
                    'column_length': column_length,
                    'column_type': row.get('ColumnType', 'CF')
                }
            )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"ALTER TABLE {x['DatabaseName']}.{x['TableName']} MODIFY COLUMN {x['ColumnName']} VARCHAR({x['ColumnLength']});",
            axis=1
        )
        
        return result_df
