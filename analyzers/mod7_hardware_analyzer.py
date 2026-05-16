"""
Module 8 Hardware Analyzer

Analyzes collected hardware utilization data and generates findings with severity levels
for the Hardware Utilization module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_analyzer import BaseAnalyzer, Severity

logger = logging.getLogger(__name__)


class HardwareAnalyzer(BaseAnalyzer):
    """
    Analyzer for Hardware Utilization Module (Module 7).
    
    Analyzes data from 2 components to identify hardware issues:
    1. AMP Space Skew - Space distribution imbalance across AMPs
    2. Node CPU Usage - CPU usage by node for current day
    """
    
    def __init__(self):
        """Initialize the Hardware Analyzer."""
        super().__init__(
            analyzer_name='HardwareAnalyzer',
            description='Analyzes Teradata hardware utilization for load balancing'
        )
        logger.info("Initialized HardwareAnalyzer")
    
    def run(self, data: Dict[str, pd.DataFrame], config: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Run analysis on collected hardware utilization data.
        
        Args:
            data: Dictionary mapping component names to DataFrames
            config: Optional configuration with thresholds
        
        Returns:
            Dictionary mapping component names to analyzed DataFrames with findings
        """
        self.clear_findings()
        analyzed_results = {}
        
        # Analyze each component
        analyzed_results['01_amp_space_skew'] = self._analyze_amp_space_skew(
            data.get('01_amp_space_skew', pd.DataFrame())
        )
        
        analyzed_results['02_node_cpu'] = self._analyze_node_cpu(
            data.get('02_node_cpu', pd.DataFrame())
        )
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_amp_space_skew(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze AMP space skew.
        
        Severity: HIGH if skew > 10%, MEDIUM if > 5%, LOW if > 2%
        DDL: Review data distribution and consider redistributing data
        """
        if df.empty:
            logger.warning("DataFrame is empty for 01_amp_space_skew")
            return df
        
        required_columns = ['AMP_ID', 'TotalSpace_Bytes']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 01_amp_space_skew. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'LOW'
        result_df['Finding_Type'] = 'AMP_SPACE_SKEW'
        result_df['Description'] = 'AMP space distribution'
        result_df['DDL_Action'] = 'REVIEW_DISTRIBUTION'
        
        # Calculate skew metrics
        max_space = result_df['TotalSpace_Bytes'].max()
        min_space = result_df['TotalSpace_Bytes'].min()
        avg_space = result_df['TotalSpace_Bytes'].mean()
        
        if min_space > 0:
            skew_percentage = ((max_space - min_space) / min_space) * 100
        else:
            skew_percentage = 0
        
        # Determine severity based on skew percentage
        if skew_percentage > 10:
            severity = 'HIGH'
            self.add_finding(
                severity=Severity.HIGH,
                finding_type='AMP_SPACE_SKEW',
                description=f"High AMP space skew detected: {skew_percentage:.2f}% difference between max and min AMP space",
                metadata={
                    'max_space_bytes': max_space,
                    'min_space_bytes': min_space,
                    'avg_space_bytes': avg_space,
                    'skew_percentage': skew_percentage,
                    'total_amps': len(result_df)
                }
            )
        elif skew_percentage > 5:
            severity = 'MEDIUM'
            self.add_finding(
                severity=Severity.MEDIUM,
                finding_type='AMP_SPACE_SKEW',
                description=f"Moderate AMP space skew detected: {skew_percentage:.2f}% difference between max and min AMP space",
                metadata={
                    'max_space_bytes': max_space,
                    'min_space_bytes': min_space,
                    'avg_space_bytes': avg_space,
                    'skew_percentage': skew_percentage,
                    'total_amps': len(result_df)
                }
            )
        elif skew_percentage > 2:
            severity = 'LOW'
            self.add_finding(
                severity=Severity.LOW,
                finding_type='AMP_SPACE_SKEW',
                description=f"Low AMP space skew detected: {skew_percentage:.2f}% difference between max and min AMP space",
                metadata={
                    'max_space_bytes': max_space,
                    'min_space_bytes': min_space,
                    'avg_space_bytes': avg_space,
                    'skew_percentage': skew_percentage,
                    'total_amps': len(result_df)
                }
            )
        else:
            severity = 'INFO'
            self.add_finding(
                severity=Severity.INFO,
                finding_type='AMP_SPACE_SKEW',
                description=f"AMP space distribution is balanced: {skew_percentage:.2f}% skew",
                metadata={
                    'max_space_bytes': max_space,
                    'min_space_bytes': min_space,
                    'avg_space_bytes': avg_space,
                    'skew_percentage': skew_percentage,
                    'total_amps': len(result_df)
                }
            )
        
        result_df['Severity'] = severity
        result_df['Skew_Percentage'] = skew_percentage
        
        # Generate DDL (suggestions)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- Review data distribution for AMP {x['AMP_ID']} (Space: {x['TotalSpace_Bytes']:,} bytes)",
            axis=1
        )
        
        return result_df
    
    def _analyze_node_cpu(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze node CPU usage.
        
        Severity: INFO if no data (suggest enabling ResUsage logging)
        DDL: No action required (informational only)
        """
        if df.empty:
            logger.warning("DataFrame is empty for 02_node_cpu")
            # Add informational finding about ResUsage logging
            self.add_finding(
                severity=Severity.INFO,
                finding_type='RESUSAGE_LOGGING_DISABLED',
                description="ResUsageSpma logging may be disabled. Consider enabling ResUsage logging for hardware monitoring.",
                metadata={
                    'suggestion': 'Enable ResUsage logging via DBQL or ResUsage configuration'
                }
            )
            return df
        
        required_columns = ['TheDate', 'NodeID', 'TotalIdle', 'TotalServ']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 02_node_cpu. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'INFO'
        result_df['Finding_Type'] = 'NODE_CPU_USAGE'
        result_df['Description'] = 'Node CPU usage metrics'
        result_df['DDL_Action'] = 'NONE'
        
        # Calculate CPU utilization percentage
        result_df['CPU_Utilization_Pct'] = (result_df['TotalServ'] / (result_df['TotalIdle'] + result_df['TotalServ'])) * 100
        
        for idx, row in result_df.iterrows():
            node_id = row['NodeID']
            cpu_util = row['CPU_Utilization_Pct']
            
            self.add_finding(
                severity=Severity.INFO,
                finding_type='NODE_CPU_USAGE',
                description=f"Node {node_id} CPU utilization: {cpu_util:.2f}%",
                metadata={
                    'node_id': node_id,
                    'date': row['TheDate'],
                    'total_idle': row['TotalIdle'],
                    'total_serv': row['TotalServ'],
                    'cpu_utilization_pct': cpu_util
                }
            )
        
        # Generate DDL (informational)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"-- Node {x['NodeID']} CPU utilization: {x['CPU_Utilization_Pct']:.2f}% on {x['TheDate']}",
            axis=1
        )
        
        return result_df
