"""
Module 1 Health Analyzer

Analyzes collected health and connectivity data and generates findings with severity levels
for the Health & Connectivity module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_analyzer import BaseAnalyzer, Severity

logger = logging.getLogger(__name__)


class HealthAnalyzer(BaseAnalyzer):
    """
    Analyzer for Health & Connectivity Module (Module 1).
    
    Analyzes data from 2 components to identify health issues:
    1. System Information - Version and release information
    2. Active Sessions - Users with high concurrent sessions
    """
    
    def __init__(self):
        """Initialize the Health Analyzer."""
        super().__init__(
            analyzer_name='HealthAnalyzer',
            description='Analyzes Teradata system health and connectivity status'
        )
        logger.info("Initialized HealthAnalyzer")
    
    def run(self, data: Dict[str, pd.DataFrame], config: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Run analysis on collected health and connectivity data.
        
        Args:
            data: Dictionary mapping component names to DataFrames
            config: Optional configuration with thresholds
        
        Returns:
            Dictionary mapping component names to analyzed DataFrames with findings
        """
        self.clear_findings()
        analyzed_results = {}
        
        # Analyze each component
        analyzed_results['01_system_info'] = self._analyze_system_info(
            data.get('01_system_info', pd.DataFrame())
        )
        
        analyzed_results['02_active_sessions'] = self._analyze_active_sessions(
            data.get('02_active_sessions', pd.DataFrame())
        )
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_system_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze system information.
        
        Severity: INFO for system metadata
        DDL: No action required (informational only)
        """
        if df.empty:
            logger.warning("DataFrame is empty for 01_system_info")
            return df
        
        required_columns = ['InfoKey', 'InfoData']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 01_system_info. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'INFO'
        result_df['Finding_Type'] = 'SYSTEM_INFO'
        result_df['Description'] = 'System metadata information'
        result_df['DDL_Action'] = 'NONE'
        
        for idx, row in result_df.iterrows():
            info_key = row['InfoKey']
            info_data = row['InfoData']
            
            self.add_finding(
                severity=Severity.INFO,
                finding_type='SYSTEM_INFO',
                description=f"System Info: {info_key} = {info_data}",
                metadata={
                    'info_key': info_key,
                    'info_data': info_data
                }
            )
        
        # Generate DDL (informational)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- System Info: {x['InfoKey']} = {x['InfoData']}",
            axis=1
        )
        
        return result_df
    
    def _analyze_active_sessions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze active sessions by user.
        
        Severity: HIGH if SessionCount > 50, MEDIUM if > 20, LOW if > 10
        DDL: Review user sessions - consider session limits
        """
        if df.empty:
            logger.warning("DataFrame is empty for 02_active_sessions")
            return df
        
        required_columns = ['UserName', 'SessionCount']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 02_active_sessions. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'LOW'
        result_df['Finding_Type'] = 'HIGH_SESSION_COUNT'
        result_df['Description'] = 'User with high concurrent sessions'
        result_df['DDL_Action'] = 'REVIEW_SESSIONS'
        
        for idx, row in result_df.iterrows():
            user_name = row['UserName']
            session_count = row['SessionCount']
            
            # Determine severity based on session count
            if session_count > 50:
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='HIGH_SESSION_COUNT',
                    description=f"User {user_name} has {session_count} concurrent sessions - critical",
                    metadata={
                        'username': user_name,
                        'session_count': session_count
                    }
                )
            elif session_count > 20:
                result_df.at[idx, 'Severity'] = 'MEDIUM'
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='HIGH_SESSION_COUNT',
                    description=f"User {user_name} has {session_count} concurrent sessions - review",
                    metadata={
                        'username': user_name,
                        'session_count': session_count
                    }
                )
            elif session_count > 10:
                self.add_finding(
                    severity=Severity.LOW,
                    finding_type='HIGH_SESSION_COUNT',
                    description=f"User {user_name} has {session_count} concurrent sessions - monitor",
                    metadata={
                        'username': user_name,
                        'session_count': session_count
                    }
                )
        
        # Generate DDL (suggestions)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- Review session limits for user {x['UserName']} (Current: {x['SessionCount']} sessions)",
            axis=1
        )
        
        return result_df
