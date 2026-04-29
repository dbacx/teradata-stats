"""
Module 2 Statistics Analyzer

Analyzes collected statistics data and generates findings with severity levels
and DDL remediation statements for the Statistics Management module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_analyzer import BaseAnalyzer, Severity
from core.config import THRESHOLDS

logger = logging.getLogger(__name__)


class StatsAnalyzer(BaseAnalyzer):
    """
    Analyzer for Statistics Management Module (Module 2).
    
    Analyzes data from 10 components to identify statistics issues:
    1. Unused Objects - Stats on tables with no recent usage
    2. Sample Candidates - Columns that should use USING SAMPLE
    3. Missing at PARTITION level - PPI tables without PARTITION stats
    4. Missing at Table Level - Tables without any stats
    5. Missing at Index Level - Indexes without stats
    6. Stale Stats - Stats with old collection timestamps
    7. Zero Stats - Stats with RowCount=0 on populated tables
    8. Multicolumn - Multicolumn stats with MaxValueLength issues
    9. Skipped and Sample - Stats being skipped or using sample
    10. DBC Recommendations - Missing stats in system databases
    """
    
    def __init__(self):
        """Initialize the Stats Analyzer."""
        super().__init__(
            analyzer_name='StatsAnalyzer',
            description='Analyzes Teradata statistics for optimization opportunities'
        )
        logger.info("Initialized StatsAnalyzer")
    
    def run(self, data: Dict[str, pd.DataFrame], config: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Run analysis on collected statistics data.
        
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
        analyzed_results['01_unused_objects'] = self._analyze_unused_objects(
            data.get('01_unused_objects', pd.DataFrame())
        )
        
        analyzed_results['02_sample_candidates'] = self._analyze_sample_candidates(
            data.get('02_sample_candidates', pd.DataFrame())
        )
        
        analyzed_results['03_missing_partition'] = self._analyze_missing_partition(
            data.get('03_missing_partition', pd.DataFrame())
        )
        
        analyzed_results['04_missing_table'] = self._analyze_missing_table(
            data.get('04_missing_table', pd.DataFrame())
        )
        
        analyzed_results['05_missing_index'] = self._analyze_missing_index(
            data.get('05_missing_index', pd.DataFrame())
        )
        
        analyzed_results['06_stale_stats'] = self._analyze_stale_stats(
            data.get('06_stale_stats', pd.DataFrame())
        )
        
        analyzed_results['07_zero_stats'] = self._analyze_zero_stats(
            data.get('07_zero_stats', pd.DataFrame())
        )
        
        analyzed_results['08_multicolumn'] = self._analyze_multicolumn(
            data.get('08_multicolumn', pd.DataFrame())
        )
        
        analyzed_results['09_skipped_sample'] = self._analyze_skipped_sample(
            data.get('09_skipped_sample', pd.DataFrame())
        )
        
        analyzed_results['10_dbc_recommendations'] = self._analyze_dbc_recommendations(
            data.get('10_dbc_recommendations', pd.DataFrame())
        )
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_unused_objects(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze unused objects - stats on tables with no recent usage.
        
        Severity: HIGH for large tables (>1GB), MEDIUM for smaller tables
        DDL: DROP STATISTICS
        """
        if df.empty:
            logger.warning("DataFrame is empty for 01_unused_objects")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'Size_GB']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 01_unused_objects. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'UNUSED_STATS'
        result_df['Description'] = 'Statistics on table with no recent usage'
        result_df['DDL_Action'] = 'DROP'
        
        # Assign severity based on size
        for idx, row in result_df.iterrows():
            size_gb = row.get('Size_GB', 0)
            if size_gb > 1.0:
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='UNUSED_STATS',
                    description=f"Large table ({size_gb:.2f} GB) {row['DatabaseName']}.{row['TableName']} has stats but no recent usage",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb,
                        'last_stat_collect': str(row.get('Last_Stat_Collect', 'N/A'))
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='UNUSED_STATS',
                    description=f"Table {row['DatabaseName']}.{row['TableName']} has stats but no recent usage",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb
                    }
                )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: self.generate_ddl_drop(x['DatabaseName'], x['TableName'], '*'),
            axis=1
        )
        
        return result_df
    
    def _analyze_sample_candidates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze sample candidates - columns that should use USING SAMPLE.
        
        Severity: INFO (optimization opportunity)
        DDL: COLLECT STATISTICS ... USING SAMPLE
        """
        if df.empty:
            logger.warning("DataFrame is empty for 02_sample_candidates")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'ColumnName', 'Size_GB', 'Uniqueness_Ratio', 'SampleSizePct']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 02_sample_candidates. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'INFO'
        result_df['Finding_Type'] = 'SAMPLE_CANDIDATE'
        result_df['Description'] = 'Column should use USING SAMPLE instead of FULL SCAN'
        result_df['DDL_Action'] = 'COLLECT SAMPLE'
        
        for idx, row in result_df.iterrows():
            self.add_finding(
                severity=Severity.INFO,
                finding_type='SAMPLE_CANDIDATE',
                description=f"Column {row['DatabaseName']}.{row['TableName']}.{row['ColumnName']} ({row['Size_GB']:.2f} GB) should use SAMPLE (uniqueness: {row['Uniqueness_Ratio']:.4f})",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'column': row['ColumnName'],
                    'size_gb': row['Size_GB'],
                    'uniqueness_ratio': row['Uniqueness_Ratio'],
                    'current_sample_pct': row['SampleSizePct']
                }
            )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"COLLECT STATISTICS USING SAMPLE {x['DatabaseName']}.{x['TableName']} COLUMN ({x['ColumnName']});",
            axis=1
        )
        
        return result_df
    
    def _analyze_missing_partition(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze missing PARTITION stats on PPI tables.
        
        Severity: CRITICAL for high partition levels (>1), HIGH otherwise
        DDL: COLLECT STATISTICS ... COLUMN (PARTITION)
        """
        if df.empty:
            logger.warning("DataFrame is empty for 03_missing_partition")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'PartitioningLevels']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 03_missing_partition. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'HIGH'
        result_df['Finding_Type'] = 'MISSING_PARTITION_STATS'
        result_df['Description'] = 'PPI table missing PARTITION column statistics'
        result_df['DDL_Action'] = 'COLLECT'
        
        for idx, row in result_df.iterrows():
            partition_levels = row.get('PartitioningLevels', 1)
            if partition_levels > 1:
                result_df.at[idx, 'Severity'] = 'CRITICAL'
                self.add_finding(
                    severity=Severity.CRITICAL,
                    finding_type='MISSING_PARTITION_STATS',
                    description=f"PPI table {row['DatabaseName']}.{row['TableName']} ({partition_levels} levels) missing PARTITION stats",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'partitioning_levels': partition_levels
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='MISSING_PARTITION_STATS',
                    description=f"PPI table {row['DatabaseName']}.{row['TableName']} missing PARTITION stats",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'partitioning_levels': partition_levels
                    }
                )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: self.generate_ddl_collect(x['DatabaseName'], x['TableName'], 'PARTITION'),
            axis=1
        )
        
        return result_df
    
    def _analyze_missing_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze tables without any statistics.
        
        Severity: HIGH (tables used yesterday but no stats)
        DDL: COLLECT STATISTICS
        """
        if df.empty:
            logger.warning("DataFrame is empty for 04_missing_table")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'TableKind']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 04_missing_table. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'HIGH'
        result_df['Finding_Type'] = 'MISSING_TABLE_STATS'
        result_df['Description'] = 'Table used recently but has no statistics'
        result_df['DDL_Action'] = 'COLLECT'
        
        for idx, row in result_df.iterrows():
            self.add_finding(
                severity=Severity.HIGH,
                finding_type='MISSING_TABLE_STATS',
                description=f"Table {row['DatabaseName']}.{row['TableName']} was used yesterday but has no statistics",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'table_kind': row['TableKind']
                }
            )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"COLLECT STATISTICS {x['DatabaseName']}.{x['TableName']};",
            axis=1
        )
        
        return result_df
    
    def _analyze_missing_index(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze indexes without statistics.
        
        Severity: HIGH for PI, MEDIUM for SI/JI
        DDL: COLLECT STATISTICS ... COLUMN (index_columns)
        """
        if df.empty:
            logger.warning("DataFrame is empty for 05_missing_index")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'IndexType', 'IndexName']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 05_missing_index. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'MISSING_INDEX_STATS'
        result_df['Description'] = 'Index missing statistics'
        result_df['DDL_Action'] = 'COLLECT'
        
        for idx, row in result_df.iterrows():
            index_type = row.get('IndexType', 'S')
            if index_type == 'P':
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='MISSING_INDEX_STATS',
                    description=f"Primary Index on {row['DatabaseName']}.{row['TableName']} missing statistics",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'index_type': index_type,
                        'index_name': row.get('IndexName', 'N/A')
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='MISSING_INDEX_STATS',
                    description=f"{index_type} Index on {row['DatabaseName']}.{row['TableName']} missing statistics",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'index_type': index_type,
                        'index_name': row.get('IndexName', 'N/A')
                    }
                )
        
        # Generate DDL (placeholder - would need actual column names)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"COLLECT STATISTICS {x['DatabaseName']}.{x['TableName']} INDEX ({x['IndexName']});",
            axis=1
        )
        
        return result_df
    
    def _analyze_stale_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze stale statistics.
        
        Severity: MEDIUM
        DDL: COLLECT STATISTICS (refresh)
        """
        if df.empty:
            logger.warning("DataFrame is empty for 06_stale_stats")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'Last_Collect_Date']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 06_stale_stats. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'STALE_STATS'
        result_df['Description'] = 'Statistics collection is stale'
        result_df['DDL_Action'] = 'REFRESH'
        
        for idx, row in result_df.iterrows():
            self.add_finding(
                severity=Severity.MEDIUM,
                finding_type='STALE_STATS',
                description=f"Statistics on {row['DatabaseName']}.{row['TableName']} last collected on {row['Last_Collect_Date']}",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'last_collect_date': str(row['Last_Collect_Date'])
                }
            )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"COLLECT STATISTICS {x['DatabaseName']}.{x['TableName']};",
            axis=1
        )
        
        return result_df
    
    def _analyze_zero_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze zero count statistics on populated tables.
        
        Severity: CRITICAL (can cause Product Join disasters)
        DDL: COLLECT STATISTICS (refresh)
        """
        if df.empty:
            logger.warning("DataFrame is empty for 07_zero_stats")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'Stats_RowCount', 'Actual_Size_GB']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 07_zero_stats. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'CRITICAL'
        result_df['Finding_Type'] = 'ZERO_STATS'
        result_df['Description'] = 'Statistics show RowCount=0 but table contains data'
        result_df['DDL_Action'] = 'REFRESH'
        
        for idx, row in result_df.iterrows():
            self.add_finding(
                severity=Severity.CRITICAL,
                finding_type='ZERO_STATS',
                description=f"CRITICAL: {row['DatabaseName']}.{row['TableName']} has zero count stats but actual size is {row['Actual_Size_GB']:.2f} GB",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'stats_rowcount': row['Stats_RowCount'],
                    'actual_size_gb': row['Actual_Size_GB']
                }
            )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"COLLECT STATISTICS {x['DatabaseName']}.{x['TableName']};",
            axis=1
        )
        
        return result_df
    
    def _analyze_multicolumn(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze multicolumn stats with MaxValueLength issues.
        
        Severity: LOW
        DDL: COLLECT STATISTICS ... WITH MAXVALUELENGTH
        """
        if df.empty:
            logger.warning("DataFrame is empty for 08_multicolumn")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'ColumnName', 'ExpressionCount', 'MaxValueLength']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 08_multicolumn. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'LOW'
        result_df['Finding_Type'] = 'MULTICOLUMN_MAXVALUELENGTH'
        result_df['Description'] = 'Multicolumn stats may be truncated due to low MaxValueLength'
        result_df['DDL_Action'] = 'RECREATE'
        
        for idx, row in result_df.iterrows():
            self.add_finding(
                severity=Severity.LOW,
                finding_type='MULTICOLUMN_MAXVALUELENGTH',
                description=f"Multicolumn stats on {row['DatabaseName']}.{row['TableName']} ({row['ColumnName']}) have MaxValueLength={row['MaxValueLength']} (ExpressionCount={row['ExpressionCount']})",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'column': row['ColumnName'],
                    'expression_count': row['ExpressionCount'],
                    'max_value_length': row['MaxValueLength']
                }
            )
        
        # Generate DDL (placeholder - would need to reconstruct full statement)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- Recollect stats on {x['DatabaseName']}.{x['TableName']} with increased MAXVALUELENGTH for {x['ColumnName']}",
            axis=1
        )
        
        return result_df
    
    def _analyze_skipped_sample(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze skipped and sample statistics.
        
        Severity: INFO
        DDL: Review and potentially adjust
        """
        if df.empty:
            logger.warning("DataFrame is empty for 09_skipped_sample")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'SampleSizePct', 'StatsSkipCount']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 09_skipped_sample. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'INFO'
        result_df['Finding_Type'] = 'SKIPPED_OR_SAMPLE'
        result_df['Description'] = 'Statistics being skipped or using sample'
        result_df['DDL_Action'] = 'REVIEW'
        
        for idx, row in result_df.iterrows():
            if row['StatsSkipCount'] > 0:
                desc = f"Statistics on {row['DatabaseName']}.{row['TableName']} skipped {row['StatsSkipCount']} times"
            else:
                desc = f"Statistics on {row['DatabaseName']}.{row['TableName']} using sample ({row['SampleSizePct']}%)"
            
            self.add_finding(
                severity=Severity.INFO,
                finding_type='SKIPPED_OR_SAMPLE',
                description=desc,
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'sample_size_pct': row['SampleSizePct'],
                    'stats_skip_count': row['StatsSkipCount'],
                    'sample_signature': row.get('SampleSignature', 'N/A')
                }
            )
        
        result_df['DDL_Statement'] = '-- Review sample/skip settings and consider COLLECT STATISTICS'
        
        return result_df
    
    def _analyze_dbc_recommendations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze missing stats in system databases.
        
        Severity: MEDIUM
        DDL: COLLECT STATISTICS on system tables
        """
        if df.empty:
            logger.warning("DataFrame is empty for 10_dbc_recommendations")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'TableKind']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 10_dbc_recommendations. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'MISSING_SYSTEM_STATS'
        result_df['Description'] = 'System table missing recommended statistics'
        result_df['DDL_Action'] = 'COLLECT'
        
        for idx, row in result_df.iterrows():
            self.add_finding(
                severity=Severity.MEDIUM,
                finding_type='MISSING_SYSTEM_STATS',
                description=f"System table {row['DatabaseName']}.{row['TableName']} missing statistics",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'table_kind': row['TableKind']
                }
            )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"COLLECT STATISTICS {x['DatabaseName']}.{x['TableName']};",
            axis=1
        )
        
        return result_df
