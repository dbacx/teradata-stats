"""
Module 3 Performance Analyzer

Analyzes collected performance data and generates findings with severity levels
and DDL remediation statements for the Performance & DDL Assessment module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_analyzer import BaseAnalyzer, Severity
from core.config import THRESHOLDS

logger = logging.getLogger(__name__)


class PerformanceAnalyzer(BaseAnalyzer):
    """
    Analyzer for Performance Assessment Module (Module 3).
    
    Analyzes data from 4 components to identify performance issues:
    1. Full Table Scans - Tables with high I/O usage
    2. Highly Skewed Queries - Queries with high CPU skew
    3. Spool Usage Alerts - Queries with spool issues
    4. Unused Indexes - Secondary indexes on large tables for review
    """
    
    def __init__(self):
        """Initialize the Performance Analyzer."""
        super().__init__(
            analyzer_name='PerformanceAnalyzer',
            description='Analyzes Teradata performance metrics for optimization opportunities'
        )
        logger.info("Initialized PerformanceAnalyzer")
    
    def run(self, data: Dict[str, pd.DataFrame], config: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Run analysis on collected performance data.
        
        Args:
            data: Dictionary mapping component names to DataFrames
            config: Optional configuration with thresholds
        
        Returns:
            Dictionary mapping component names to analyzed DataFrames with findings
        """
        # Use config thresholds if provided, otherwise use defaults
        if config is None:
            config = THRESHOLDS.copy()
        
        self.clear_findings()
        analyzed_results = {}
        
        # Analyze each component
        analyzed_results['01_full_table_scans'] = self._analyze_full_table_scans(
            data.get('01_full_table_scans', pd.DataFrame())
        )
        
        analyzed_results['02_highly_skewed_queries'] = self._analyze_highly_skewed_queries(
            data.get('02_highly_skewed_queries', pd.DataFrame())
        )
        
        analyzed_results['03_spool_usage_alerts'] = self._analyze_spool_usage_alerts(
            data.get('03_spool_usage_alerts', pd.DataFrame())
        )
        
        analyzed_results['04_unused_indexes'] = self._analyze_unused_indexes(
            data.get('04_unused_indexes', pd.DataFrame())
        )
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_full_table_scans(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze full table scans (large tables by size).
        
        Severity: CRITICAL if Size_GB > 100, HIGH if > 50, MEDIUM if > 10
        DDL: Consider adding indexes or statistics
        """
        if df.empty:
            logger.warning("DataFrame is empty for 01_full_table_scans")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'Size_GB']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 01_full_table_scans. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'LARGE_TABLE'
        result_df['Description'] = 'Large table - potential FTS risk'
        result_df['DDL_Action'] = 'REVIEW_INDEX'
        
        for idx, row in result_df.iterrows():
            size_gb = row['Size_GB']
            table_name = f"{row['DatabaseName']}.{row['TableName']}"
            
            # Determine severity based on Size_GB
            if size_gb > 100:  # > 100GB
                result_df.at[idx, 'Severity'] = 'CRITICAL'
                self.add_finding(
                    severity=Severity.CRITICAL,
                    finding_type='LARGE_TABLE',
                    description=f"Table {table_name} is extremely large ({size_gb:.2f} GB) - high FTS risk",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb,
                        'peak_size_gb': row.get('PeakSize_GB', 0)
                    }
                )
            elif size_gb > 50:  # > 50GB
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='LARGE_TABLE',
                    description=f"Table {table_name} is large ({size_gb:.2f} GB) - review access patterns",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='LARGE_TABLE',
                    description=f"Table {table_name} is moderate size ({size_gb:.2f} GB) - monitor",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb
                    }
                )
        
        # Generate DDL (suggestions)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- Review indexes for {x['DatabaseName']}.{x['TableName']} (Size: {x['Size_GB']:.2f} GB)",
            axis=1
        )
        
        return result_df
    
    def _analyze_highly_skewed_queries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze highly skewed queries (large tables by size).
        
        Severity: CRITICAL if Size_GB > 100, HIGH if > 50, MEDIUM if > 5
        DDL: Review data distribution or statistics
        """
        if df.empty:
            logger.warning("DataFrame is empty for 02_highly_skewed_queries")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'Size_GB']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 02_highly_skewed_queries. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'SKEW_RISK'
        result_df['Description'] = 'Large table - potential skew risk'
        result_df['DDL_Action'] = 'REVIEW_DISTRIBUTION'
        
        for idx, row in result_df.iterrows():
            size_gb = row['Size_GB']
            table_name = f"{row['DatabaseName']}.{row['TableName']}"
            
            # Determine severity based on Size_GB
            if size_gb > 100:  # > 100GB
                result_df.at[idx, 'Severity'] = 'CRITICAL'
                self.add_finding(
                    severity=Severity.CRITICAL,
                    finding_type='SKEW_RISK',
                    description=f"Table {table_name} is extremely large ({size_gb:.2f} GB) - high skew risk",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb,
                        'recommendation': row.get('Recommendation', 'REVIEW_SKEW')
                    }
                )
            elif size_gb > 50:  # > 50GB
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='SKEW_RISK',
                    description=f"Table {table_name} is large ({size_gb:.2f} GB) - review data distribution",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='SKEW_RISK',
                    description=f"Table {table_name} is moderate size ({size_gb:.2f} GB) - monitor",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb
                    }
                )
        
        # Generate DDL (suggestions)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- Review data distribution for {x['DatabaseName']}.{x['TableName']} (Size: {x['Size_GB']:.2f} GB)",
            axis=1
        )
        
        return result_df
    
    def _analyze_spool_usage_alerts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze spool usage alerts (large tables by size).
        
        Severity: CRITICAL if Size_GB > 100, HIGH if > 50, MEDIUM if > 10
        DDL: Review spool allocation or optimize queries
        """
        if df.empty:
            logger.warning("DataFrame is empty for 03_spool_usage_alerts")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'Size_GB']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 03_spool_usage_alerts. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'SPOOL_RISK'
        result_df['Description'] = 'Large table - potential spool risk'
        result_df['DDL_Action'] = 'REVIEW_SPOOL'
        
        for idx, row in result_df.iterrows():
            size_gb = row['Size_GB']
            table_name = f"{row['DatabaseName']}.{row['TableName']}"
            
            # Determine severity based on Size_GB
            if size_gb > 100:  # > 100GB
                result_df.at[idx, 'Severity'] = 'CRITICAL'
                self.add_finding(
                    severity=Severity.CRITICAL,
                    finding_type='SPOOL_RISK',
                    description=f"Table {table_name} is extremely large ({size_gb:.2f} GB) - high spool risk",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb,
                        'recommendation': row.get('Recommendation', 'REVIEW_SPOOL')
                    }
                )
            elif size_gb > 50:  # > 50GB
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='SPOOL_RISK',
                    description=f"Table {table_name} is large ({size_gb:.2f} GB) - review spool allocation",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='SPOOL_RISK',
                    description=f"Table {table_name} is moderate size ({size_gb:.2f} GB) - monitor",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb
                    }
                )
        
        # Generate DDL (suggestions)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- Review spool allocation for queries on {x['DatabaseName']}.{x['TableName']} (Size: {x['Size_GB']:.2f} GB)",
            axis=1
        )
        
        return result_df
    
    def _analyze_unused_indexes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze unused/underutilized indexes.
        
        Severity: HIGH for SI/NUSI on tables > 50GB, MEDIUM for tables > 10GB
        DDL: DROP INDEX or review index usage
        """
        if df.empty:
            logger.warning("DataFrame is empty for 04_unused_indexes")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'IndexName', 'IndexType', 'TableSize_GB']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 04_unused_indexes. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'UNUSED_INDEX'
        result_df['Description'] = 'Secondary index on large table for review'
        result_df['DDL_Action'] = 'DROP_INDEX'
        
        for idx, row in result_df.iterrows():
            table_size = row['TableSize_GB']
            index_type = row['IndexType']
            index_name = f"{row['DatabaseName']}.{row['TableName']}.{row['IndexName']}"
            
            # Determine severity based on table size
            if table_size > 50:
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='UNUSED_INDEX',
                    description=f"Index {index_name} ({index_type}) on large table ({table_size:.2f} GB) - review usage",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'index_name': row['IndexName'],
                        'index_type': index_type,
                        'table_size_gb': table_size,
                        'columns': row.get('ColumnNames', '')
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='UNUSED_INDEX',
                    description=f"Index {index_name} ({index_type}) on table ({table_size:.2f} GB) - review usage",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'index_name': row['IndexName'],
                        'index_type': index_type,
                        'table_size_gb': table_size
                    }
                )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"DROP INDEX {x['IndexName']} ON {x['DatabaseName']}.{x['TableName']}; -- Review before dropping",
            axis=1
        )
        
        return result_df
