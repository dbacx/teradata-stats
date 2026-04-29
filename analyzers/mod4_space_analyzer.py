"""
Module 4 Space Analyzer

Analyzes collected space data and generates findings with severity levels
and DDL remediation statements for the Space Assessment module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_analyzer import BaseAnalyzer, Severity
from core.config import THRESHOLDS

logger = logging.getLogger(__name__)


class SpaceAnalyzer(BaseAnalyzer):
    """
    Analyzer for Space Assessment Module (Module 4).
    
    Analyzes data from 5 components to identify space issues:
    1. Database Space Utilization - Databases with high space usage
    2. Unused Tables Space - Tables with no recent access consuming space
    3. MVC Candidates - Compressible columns without compression
    4. Top Tables - Ranking of largest tables
    5. Skewed Tables - Tables with data skew across AMPs
    """
    
    def __init__(self):
        """Initialize the Space Analyzer."""
        super().__init__(
            analyzer_name='SpaceAnalyzer',
            description='Analyzes Teradata space usage for optimization opportunities'
        )
        logger.info("Initialized SpaceAnalyzer")
    
    def run(self, data: Dict[str, pd.DataFrame], config: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Run analysis on collected space data.
        
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
        analyzed_results['01_db_space_utilization'] = self._analyze_db_space_utilization(
            data.get('01_db_space_utilization', pd.DataFrame())
        )
        
        analyzed_results['02_unused_tables_space'] = self._analyze_unused_tables_space(
            data.get('02_unused_tables_space', pd.DataFrame())
        )
        
        analyzed_results['03_mvc_candidates'] = self._analyze_mvc_candidates(
            data.get('03_mvc_candidates', pd.DataFrame())
        )
        
        analyzed_results['04_top_tables'] = self._analyze_top_tables(
            data.get('04_top_tables', pd.DataFrame())
        )
        
        analyzed_results['05_skewed_tables'] = self._analyze_skewed_tables(
            data.get('05_skewed_tables', pd.DataFrame())
        )
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_db_space_utilization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze database space utilization.
        
        Severity: CRITICAL if usage > 80%, HIGH if > 60%, MEDIUM if > 40%
        DDL: No direct DDL, requires capacity planning
        """
        if df.empty:
            logger.warning("DataFrame is empty for 01_db_space_utilization")
            return df
        
        required_columns = ['DatabaseName', 'Usage_Pct', 'CurrentPerm_TB', 'MaxPerm_TB']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 01_db_space_utilization. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'INFO'
        result_df['Finding_Type'] = 'DB_SPACE_UTILIZATION'
        result_df['Description'] = 'Database space utilization'
        result_df['DDL_Action'] = 'MONITOR'
        
        for idx, row in result_df.iterrows():
            usage_pct = row.get('Usage_Pct', 0)
            
            if usage_pct > 80:
                result_df.at[idx, 'Severity'] = 'CRITICAL'
                self.add_finding(
                    severity=Severity.CRITICAL,
                    finding_type='DB_SPACE_UTILIZATION',
                    description=f"Database {row['DatabaseName']} is at {usage_pct:.2f}% capacity ({row['CurrentPerm_TB']:.2f} TB / {row['MaxPerm_TB']:.2f} TB)",
                    metadata={
                        'database': row['DatabaseName'],
                        'usage_pct': usage_pct,
                        'current_perm_tb': row['CurrentPerm_TB'],
                        'max_perm_tb': row['MaxPerm_TB']
                    }
                )
            elif usage_pct > 60:
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='DB_SPACE_UTILIZATION',
                    description=f"Database {row['DatabaseName']} is at {usage_pct:.2f}% capacity ({row['CurrentPerm_TB']:.2f} TB / {row['MaxPerm_TB']:.2f} TB)",
                    metadata={
                        'database': row['DatabaseName'],
                        'usage_pct': usage_pct,
                        'current_perm_tb': row['CurrentPerm_TB'],
                        'max_perm_tb': row['MaxPerm_TB']
                    }
                )
            elif usage_pct > 40:
                result_df.at[idx, 'Severity'] = 'MEDIUM'
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='DB_SPACE_UTILIZATION',
                    description=f"Database {row['DatabaseName']} is at {usage_pct:.2f}% capacity",
                    metadata={
                        'database': row['DatabaseName'],
                        'usage_pct': usage_pct,
                        'current_perm_tb': row['CurrentPerm_TB']
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.INFO,
                    finding_type='DB_SPACE_UTILIZATION',
                    description=f"Database {row['DatabaseName']} is at {usage_pct:.2f}% capacity",
                    metadata={
                        'database': row['DatabaseName'],
                        'usage_pct': usage_pct
                    }
                )
        
        result_df['DDL_Statement'] = '-- Monitor database space and plan capacity expansion'
        
        return result_df
    
    def _analyze_unused_tables_space(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze unused tables consuming space.
        
        Severity: HIGH for tables > 1GB, MEDIUM for smaller tables
        DDL: DROP TABLE
        """
        if df.empty:
            logger.warning("DataFrame is empty for 02_unused_tables_space")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'Size_GB']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 02_unused_tables_space. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'UNUSED_TABLE_SPACE'
        result_df['Description'] = 'Table with no recent access consuming space'
        result_df['DDL_Action'] = 'DROP'
        
        for idx, row in result_df.iterrows():
            size_gb = row.get('Size_GB', 0)
            last_access = row.get('LastAccessTimeStamp', 'Never')
            
            if size_gb > 1.0:
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='UNUSED_TABLE_SPACE',
                    description=f"Table {row['DatabaseName']}.{row['TableName']} ({size_gb:.2f} GB) has no recent access (last: {last_access})",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb,
                        'last_access': str(last_access),
                        'table_kind': row['TableKind']
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='UNUSED_TABLE_SPACE',
                    description=f"Table {row['DatabaseName']}.{row['TableName']} ({size_gb:.2f} GB) has no recent access",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'size_gb': size_gb,
                        'last_access': str(last_access)
                    }
                )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"DROP TABLE {x['DatabaseName']}.{x['TableName']};",
            axis=1
        )
        
        return result_df
    
    def _analyze_mvc_candidates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze MVC (Multi-Value Compression) candidates.
        
        Severity: INFO (optimization opportunity)
        DDL: ALTER TABLE ... ADD ... COMPRESS
        """
        if df.empty:
            logger.warning("DataFrame is empty for 03_mvc_candidates")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'ColumnName', 'ColumnType', 'TableSize_GB']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 03_mvc_candidates. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'INFO'
        result_df['Finding_Type'] = 'MVC_CANDIDATE'
        result_df['Description'] = 'Compressible column without MVC'
        result_df['DDL_Action'] = 'ADD_COMPRESS'
        
        for idx, row in result_df.iterrows():
            column_type_map = {
                'CF': 'CHAR', 'CV': 'VARCHAR', 'I1': 'BYTEINT',
                'I2': 'SMALLINT', 'I4': 'INTEGER', 'I8': 'BIGINT',
                'D': 'DATE', 'DA': 'DATE'
            }
            type_name = column_type_map.get(row['ColumnType'], row['ColumnType'])
            
            self.add_finding(
                severity=Severity.INFO,
                finding_type='MVC_CANDIDATE',
                description=f"Column {row['DatabaseName']}.{row['TableName']}.{row['ColumnName']} ({type_name}) in {row['TableSize_GB']:.2f} GB table is a compression candidate",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'column': row['ColumnName'],
                    'column_type': type_name,
                    'column_length': row['ColumnLength'],
                    'table_size_gb': row['TableSize_GB']
                }
            )
        
        # Generate DDL template
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"ALTER TABLE {x['DatabaseName']}.{x['TableName']} ADD {x['ColumnName']} COMPRESS ('value1', 'value2');  -- Review compression values",
            axis=1
        )
        
        return result_df
    
    def _analyze_top_tables(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze top tables by size.
        
        Severity: INFO (informational)
        DDL: No direct DDL, requires monitoring
        """
        if df.empty:
            logger.warning("DataFrame is empty for 04_top_tables")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'Size_GB', 'PeakSize_GB', 'AMP_Count']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 04_top_tables. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'INFO'
        result_df['Finding_Type'] = 'TOP_TABLE'
        result_df['Description'] = 'Large table requiring monitoring'
        result_df['DDL_Action'] = 'MONITOR'
        
        for idx, row in result_df.iterrows():
            self.add_finding(
                severity=Severity.INFO,
                finding_type='TOP_TABLE',
                description=f"Table {row['DatabaseName']}.{row['TableName']} is {row['Size_GB']:.2f} GB (Peak: {row['PeakSize_GB']:.2f} GB, AMPs: {row['AMP_Count']})",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'size_gb': row['Size_GB'],
                    'peak_size_gb': row['PeakSize_GB'],
                    'amp_count': row['AMP_Count']
                }
            )
        
        result_df['DDL_Statement'] = '-- Monitor large table for optimization opportunities'
        
        return result_df
    
    def _analyze_skewed_tables(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze skewed tables.
        
        Severity: CRITICAL if skew > 50%, HIGH if > 30%, MEDIUM if > 20%
        DDL: Requires table redesign or statistics
        """
        if df.empty:
            logger.warning("DataFrame is empty for 05_skewed_tables")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'Skew_Pct', 'AMP_Count']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 05_skewed_tables. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'SKEWED_TABLE'
        result_df['Description'] = 'Table with data skew across AMPs'
        result_df['DDL_Action'] = 'REDESIGN'
        
        for idx, row in result_df.iterrows():
            skew_pct = row.get('Skew_Pct', 0)
            
            if skew_pct > 50:
                result_df.at[idx, 'Severity'] = 'CRITICAL'
                self.add_finding(
                    severity=Severity.CRITICAL,
                    finding_type='SKEWED_TABLE',
                    description=f"CRITICAL: Table {row['DatabaseName']}.{row['TableName']} has {skew_pct:.2f}% skew (Max: {row['Max_CurrentPerm_Bytes']:,}, Avg: {row['Avg_CurrentPerm_Bytes']:,})",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'skew_pct': skew_pct,
                        'max_current_perm': row['Max_CurrentPerm_Bytes'],
                        'avg_current_perm': row['Avg_CurrentPerm_Bytes'],
                        'min_current_perm': row['Min_CurrentPerm_Bytes'],
                        'amp_count': row['AMP_Count']
                    }
                )
            elif skew_pct > 30:
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='SKEWED_TABLE',
                    description=f"Table {row['DatabaseName']}.{row['TableName']} has {skew_pct:.2f}% skew",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'skew_pct': skew_pct,
                        'max_current_perm': row['Max_CurrentPerm_Bytes'],
                        'avg_current_perm': row['Avg_CurrentPerm_Bytes'],
                        'amp_count': row['AMP_Count']
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='SKEWED_TABLE',
                    description=f"Table {row['DatabaseName']}.{row['TableName']} has {skew_pct:.2f}% skew",
                    metadata={
                        'database': row['DatabaseName'],
                        'table': row['TableName'],
                        'skew_pct': skew_pct,
                        'amp_count': row['AMP_Count']
                    }
                )
        
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- Review PI design for {x['DatabaseName']}.{x['TableName']} to reduce skew. Consider statistics collection.",
            axis=1
        )
        
        return result_df
