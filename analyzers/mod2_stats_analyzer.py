"""
Module 2 Statistics Analyzer

Analyzes collected statistics data and generates findings with severity levels.
Handles varying column schemas from 15 SQL queries. Identifies DDL remediation
columns (Action_SQL or RemediationDDL) dynamically.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional, List
from pathlib import Path
import sys
from core.base_analyzer import BaseAnalyzer, Severity
from core.config import THRESHOLDS

# ---------------------------------------------------------
# BULLETPROOF PATH ROUTING
# ---------------------------------------------------------
current_file_path = Path(__file__).resolve()
project_root = str(current_file_path.parent.parent)

if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger(__name__)

# Possible DDL column names across SQL files
DDL_COLUMNS = ['RemediationDDL', 'Action_SQL']

# Human-readable names per component
COMPONENT_LABELS = {
    '01_statistics_unused_objects':           'Unused Objects',
    '02_statistics_sample_candidates':        'Sample Candidates',
    '03_statistics_missing_partition':        'Missing PARTITION',
    '04_statistics_missing_table':            'Missing Table Stats',
    '05_statistics_missing_index':            'Missing Index Stats',
    '06_statistics_stale_stats':              'Stale Statistics',
    '07_statistics_zero_stats':               'Zero Statistics',
    '08_statistics_multicolumn':              'Multicolumn Issues',
    '09_statistics_skipped_sample':           'Skipped/Sample',
    '10_statistics_dbc_recommendations':      'DBC Recommendations',
    '11_statistics_mlppi_missing_levels':     'MLPPI Missing Levels',
    '12_statistics_bloat':                    'Statistics Bloat',
    '13_statistics_sampled_skew':             'Sampled Skew',
    '14_statistics_stale_by_volume':          'Stale by Volume',
    '15_tdstats_recommendations':             'TDStats Recommendations',
}

# Severity mapping per component — only 4 levels: CRITICAL, HIGH, MEDIUM, LOW
COMPONENT_SEVERITY = {
    '01_statistics_unused_objects':           Severity.MEDIUM,
    '02_statistics_sample_candidates':        Severity.LOW,
    '03_statistics_missing_partition':        Severity.HIGH,
    '04_statistics_missing_table':            Severity.HIGH,
    '05_statistics_missing_index':            Severity.MEDIUM,
    '06_statistics_stale_stats':              Severity.MEDIUM,
    '07_statistics_zero_stats':               Severity.CRITICAL,
    '08_statistics_multicolumn':              Severity.LOW,
    '09_statistics_skipped_sample':           Severity.LOW,
    '10_statistics_dbc_recommendations':      Severity.MEDIUM,
    '11_statistics_mlppi_missing_levels':     Severity.HIGH,
    '12_statistics_bloat':                    Severity.MEDIUM,
    '13_statistics_sampled_skew':             Severity.HIGH,
    '14_statistics_stale_by_volume':          Severity.HIGH,
    '15_tdstats_recommendations':             Severity.MEDIUM,
}


def _find_ddl_column(df: pd.DataFrame) -> Optional[str]:
    """Return the name of the DDL column present in the DataFrame, or None."""
    for col in DDL_COLUMNS:
        if col in df.columns:
            return col
    return None


class StatsAnalyzer(BaseAnalyzer):
    """
    Analyzer for Statistics Management Module (Module 2).
    
    Processes DataFrames from StatsCollector (15 components with varying schemas),
    assigns severity, and registers findings.
    Severity contract: CRITICAL, HIGH, MEDIUM, LOW only (no INFO).
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
            Dictionary mapping component names to analyzed DataFrames with Severity column
        """
        if config is None:
            config = THRESHOLDS.copy()
        
        self.clear_findings()
        analyzed_results = {}
        
        for component_key, severity in COMPONENT_SEVERITY.items():
            try:
                df = data.get(component_key, pd.DataFrame())
                analyzed_results[component_key] = self._analyze_component(df, component_key, severity)
            except Exception as e:
                logger.error(f"Error analyzing {component_key}: {str(e)}")
                analyzed_results[component_key] = pd.DataFrame()
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_component(self, df: pd.DataFrame, component_key: str, severity: Severity) -> pd.DataFrame:
        """
        Generic analysis for any component. Adds Severity column and registers findings.
        Works with any column schema.
        """
        if df.empty:
            logger.warning(f"DataFrame is empty for {component_key}")
            return pd.DataFrame()
        
        result_df = df.copy()
        result_df['Severity'] = severity.value
        
        label = COMPONENT_LABELS.get(component_key, component_key)
        ddl_col = _find_ddl_column(df)
        
        for _, row in result_df.iterrows():
            db_name = row.get('DatabaseName', 'N/A')
            tbl_name = row.get('TableName', 'N/A')
            obj_name = row.get('ObjectName', row.get('ColumnName', row.get('IndexName', 'TABLE LEVEL')))
            remediation = row.get(ddl_col, '') if ddl_col else ''
            
            self.add_finding(
                severity=severity,
                finding_type=label,
                description=f"{label}: {db_name}.{tbl_name} ({obj_name})",
                metadata={
                    'database': db_name,
                    'table': tbl_name,
                    'object': obj_name,
                    'category': label,
                    'remediation': remediation,
                }
            )
        
        return result_df
    
    def get_severity_summary(self) -> Dict[str, int]:
        """
        Get summary of findings by severity.
        Contract: only CRITICAL, HIGH, MEDIUM, LOW keys.
        """
        summary = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0}
        for finding in self.findings:
            sev = finding['severity']
            if sev in summary:
                summary[sev] += 1
        return summary
