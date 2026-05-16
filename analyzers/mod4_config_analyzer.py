"""
Module 5 Config Analyzer

Analyzes collected configuration and logging data and generates findings with severity levels
and DDL remediation statements for the Data Collection & Logging Config module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_analyzer import BaseAnalyzer, Severity
from core.config import THRESHOLDS

logger = logging.getLogger(__name__)


class ConfigAnalyzer(BaseAnalyzer):
    """
    Analyzer for Config Assessment Module (Module 4).
    
    Analyzes data from 4 components to identify configuration issues:
    1. ResUsage Rules - Validates ResUsage collection intervals
    2. DBQL Rules - Lists active DBQL logging rules
    3. DBQL Thresholds - Checks if thresholds are configured
    4. DBQL Tables Health - Checks health and size of DBQL logging tables
    """
    
    def __init__(self):
        """Initialize the Config Analyzer."""
        super().__init__(
            analyzer_name='ConfigAnalyzer',
            description='Analyzes Teradata configuration and logging for optimization opportunities'
        )
        logger.info("Initialized ConfigAnalyzer")
    
    def run(self, data: Dict[str, pd.DataFrame], config: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Run analysis on collected configuration data.
        
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
        analyzed_results['01_resusage_rules'] = self._analyze_resusage_rules(
            data.get('01_resusage_rules', pd.DataFrame())
        )
        
        analyzed_results['02_dbql_rules'] = self._analyze_dbql_rules(
            data.get('02_dbql_rules', pd.DataFrame())
        )
        
        analyzed_results['03_dbql_thresholds'] = self._analyze_dbql_thresholds(
            data.get('03_dbql_thresholds', pd.DataFrame())
        )
        
        analyzed_results['04_dbql_tables_health'] = self._analyze_dbql_tables_health(
            data.get('04_dbql_tables_health', pd.DataFrame())
        )
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_resusage_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze ResUsage rules.
        
        Severity: MEDIUM if NodeLoggingRate > 10 minutes, INFO otherwise
        DDL: MODIFY RESUSAGE ... (requires admin privileges)
        """
        if df.empty:
            logger.warning("DataFrame is empty for 01_resusage_rules")
            return df
        
        required_columns = ['RuleName', 'RuleValue', 'RuleType']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 01_resusage_rules. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'INFO'
        result_df['Finding_Type'] = 'RESUSAGE_CONFIG'
        result_df['Description'] = 'ResUsage configuration'
        result_df['DDL_Action'] = 'MODIFY_RESUSAGE'
        
        for idx, row in result_df.iterrows():
            rule_name = row['RuleName']
            rule_value = row['RuleValue']
            
            # Check NodeLoggingRate (interval in minutes)
            if rule_name == 'NodeLoggingRate':
                try:
                    rate = int(rule_value) if rule_value else 0
                    if rate > 10:
                        result_df.at[idx, 'Severity'] = 'MEDIUM'
                        self.add_finding(
                            severity=Severity.MEDIUM,
                            finding_type='RESUSAGE_CONFIG',
                            description=f"ResUsage NodeLoggingRate is {rate} minutes (recommended: 10 minutes or less)",
                            metadata={
                                'rule_name': rule_name,
                                'rule_value': rule_value,
                                'rule_type': row['RuleType']
                            }
                        )
                    else:
                        self.add_finding(
                            severity=Severity.INFO,
                            finding_type='RESUSAGE_CONFIG',
                            description=f"ResUsage NodeLoggingRate is {rate} minutes (acceptable)",
                            metadata={
                                'rule_name': rule_name,
                                'rule_value': rule_value
                            }
                        )
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse NodeLoggingRate value: {rule_value}")
            
            # Check ActiveFilterMode
            elif rule_name == 'ActiveFilterMode':
                self.add_finding(
                    severity=Severity.INFO,
                    finding_type='RESUSAGE_CONFIG',
                    description=f"ResUsage ActiveFilterMode is set to {rule_value}",
                    metadata={
                        'rule_name': rule_name,
                        'rule_value': rule_value
                    }
                )
            
            # Check SummaryMode
            elif rule_name == 'SummaryMode':
                self.add_finding(
                    severity=Severity.INFO,
                    finding_type='RESUSAGE_CONFIG',
                    description=f"ResUsage SummaryMode is set to {rule_value}",
                    metadata={
                        'rule_name': rule_name,
                        'rule_value': rule_value
                    }
                )
        
        # Generate DDL (placeholder - requires admin privileges)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- MODIFY RESUSAGE {x['RuleName']} AS '{x['RuleValue']}'; -- Requires admin privileges",
            axis=1
        )
        
        return result_df
    
    def _analyze_dbql_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze DBQL rules.
        
        Severity: HIGH if rules apply to 'ALL' without thresholds, MEDIUM otherwise
        DDL: BEGIN QUERY LOGGING ... or REPLACE QUERY LOGGING ...
        """
        if df.empty:
            logger.warning("DataFrame is empty for 02_dbql_rules")
            return df
        
        required_columns = ['UserName', 'AccountString', 'RuleType', 'LoggingOption', 'IsActive']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 02_dbql_rules. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'DBQL_RULE'
        result_df['Description'] = 'DBQL logging rule'
        result_df['DDL_Action'] = 'REPLACE_LOGGING'
        
        for idx, row in result_df.iterrows():
            username = row['UserName']
            account_string = row['AccountString']
            rule_type = row['RuleType']
            logging_option = row['LoggingOption']
            
            # Check if rule applies to ALL users
            if username == 'ALL':
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='DBQL_RULE',
                    description=f"DBQL rule applies to ALL users ({account_string}) with {logging_option} - ensure thresholds are configured",
                    metadata={
                        'username': username,
                        'account_string': account_string,
                        'rule_type': rule_type,
                        'logging_option': logging_option
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='DBQL_RULE',
                    description=f"DBQL rule for user {username} ({account_string}) with {logging_option}",
                    metadata={
                        'username': username,
                        'account_string': account_string,
                        'rule_type': rule_type,
                        'logging_option': logging_option
                    }
                )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"REPLACE QUERY LOGGING WITH {x['LoggingOption']} ON {x['UserName']} FOR ACCOUNT {x['AccountString']};",
            axis=1
        )
        
        return result_df
    
    def _analyze_dbql_thresholds(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze DBQL thresholds.
        
        Severity: HIGH if thresholds are missing for 'ALL' rules, MEDIUM otherwise
        DDL: REPLACE QUERY LOGGING ... LIMIT ...
        """
        if df.empty:
            logger.warning("DataFrame is empty for 03_dbql_thresholds")
            return df
        
        required_columns = ['UserName', 'AccountString', 'RuleType', 'SQLTextTime', 'SQLTextIO', 'SummaryRate']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 03_dbql_thresholds. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'DBQL_THRESHOLD'
        result_df['Description'] = 'DBQL threshold configuration'
        result_df['DDL_Action'] = 'CONFIGURE_THRESHOLD'
        
        for idx, row in result_df.iterrows():
            username = row['UserName']
            account_string = row['AccountString']
            sql_text_time = row['SQLTextTime']
            sql_text_io = row['SQLTextIO']
            summary_rate = row['SummaryRate']
            
            # Check if thresholds are configured
            if sql_text_time is None and sql_text_io is None:
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='DBQL_THRESHOLD',
                    description=f"DBQL rule for {username} ({account_string}) has no thresholds configured (SQLTextTime or SQLTextIO)",
                    metadata={
                        'username': username,
                        'account_string': account_string,
                        'sql_text_time': sql_text_time,
                        'sql_text_io': sql_text_io,
                        'summary_rate': summary_rate
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='DBQL_THRESHOLD',
                    description=f"DBQL rule for {username} ({account_string}) has thresholds: SQLTextTime={sql_text_time}, SQLTextIO={sql_text_io}",
                    metadata={
                        'username': username,
                        'account_string': account_string,
                        'sql_text_time': sql_text_time,
                        'sql_text_io': sql_text_io,
                        'summary_rate': summary_rate
                    }
                )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"REPLACE QUERY LOGGING LIMIT SQLTEXT={x['SQLTextTime'] if x['SQLTextTime'] else 0} AND SUMMARY={x['SummaryRate'] if x['SummaryRate'] else '100, 1000'} ON {x['UserName']};",
            axis=1
        )
        
        return result_df
    
    def _analyze_dbql_tables_health(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze DBQL tables health.
        
        Severity: CRITICAL if tables are missing, HIGH if tables are empty, MEDIUM if tables are large (> 100GB)
        DDL: CREATE TABLE ... or DROP TABLE ... (requires admin)
        """
        if df.empty:
            logger.warning("DataFrame is empty for 04_dbql_tables_health")
            return df
        
        required_columns = ['DatabaseName', 'TableName', 'TableKind', 'Size_GB']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 04_dbql_tables_health. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'INFO'
        result_df['Finding_Type'] = 'DBQL_TABLE_HEALTH'
        result_df['Description'] = 'DBQL logging table health'
        result_df['DDL_Action'] = 'MAINTENANCE'
        
        # Expected DBQL tables
        expected_tables = ['DBQLogTbl', 'DBQLogTbl_Hst', 'DBQLObjTbl', 'DBQLObjTbl_Hst', 'DBQLSqlTbl', 'DBQLSqlTbl_Hst']
        
        # Check if expected tables exist
        existing_tables = set(df['TableName'].tolist())
        missing_tables = set(expected_tables) - existing_tables
        
        if missing_tables:
            for table in missing_tables:
                self.add_finding(
                    severity=Severity.CRITICAL,
                    finding_type='DBQL_TABLE_HEALTH',
                    description=f"CRITICAL: Expected DBQL table {table} does not exist",
                    metadata={
                        'table_name': table,
                        'database': 'DBC'
                    }
                )
        
        for idx, row in result_df.iterrows():
            table_name = row['TableName']
            size_gb = row.get('Size_GB', 0)
            table_kind = row['TableKind']
            
            # Check table size
            if size_gb > 100:
                result_df.at[idx, 'Severity'] = 'MEDIUM'
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='DBQL_TABLE_HEALTH',
                    description=f"DBQL table {table_name} is large ({size_gb:.2f} GB) - consider archival or purging",
                    metadata={
                        'table_name': table_name,
                        'database': row['DatabaseName'],
                        'size_gb': size_gb,
                        'table_kind': table_kind
                    }
                )
            elif size_gb == 0:
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='DBQL_TABLE_HEALTH',
                    description=f"DBQL table {table_name} is empty (0 GB) - verify logging is active",
                    metadata={
                        'table_name': table_name,
                        'database': row['DatabaseName'],
                        'table_kind': table_kind
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.INFO,
                    finding_type='DBQL_TABLE_HEALTH',
                    description=f"DBQL table {table_name} is healthy ({size_gb:.2f} GB)",
                    metadata={
                        'table_name': table_name,
                        'database': row['DatabaseName'],
                        'size_gb': size_gb
                    }
                )
        
        # Generate DDL (placeholder - requires admin)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- Maintenance for {x['DatabaseName']}.{x['TableName']} (Size: {x['Size_GB']:.2f} GB) -- Requires admin privileges",
            axis=1
        )
        
        return result_df
