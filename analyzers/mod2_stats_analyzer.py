"""
Module 2 Statistics Analyzer

Analyzes collected statistics data and generates findings with severity levels.
Works with the unified 6-column SQL schema:
  DatabaseName, TableName, ObjectName, FindingCategory, LastCollectTimeStamp, RemediationDDL
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
# Sube exactamente 1 nivel desde analyzers/ hasta teradata-stats/
current_file_path = Path(__file__).resolve()
project_root = str(current_file_path.parent.parent)

if project_root not in sys.path:
    sys.path.insert(0, project_root)  # insert(0) fuerza a Python a buscar aquí primero

logger = logging.getLogger(__name__)

# Unified schema columns expected from all SQL queries
UNIFIED_COLUMNS = ['DatabaseName', 'TableName', 'ObjectName', 'FindingCategory', 'LastCollectTimeStamp', 'RemediationDDL']

# Severity mapping per component — only 4 levels allowed: CRITICAL, HIGH, MEDIUM, LOW
COMPONENT_SEVERITY = {
    '01_unused_objects':      Severity.MEDIUM,
    '02_sample_candidates':   Severity.LOW,
    '03_missing_partition':   Severity.HIGH,
    '04_missing_table':       Severity.HIGH,
    '05_missing_index':       Severity.MEDIUM,
    '06_stale_stats':         Severity.MEDIUM,
    '07_zero_stats':          Severity.CRITICAL,
    '08_multicolumn':         Severity.LOW,
    '09_skipped_sample':      Severity.LOW,
    '10_dbc_recommendations': Severity.MEDIUM,
}


class StatsAnalyzer(BaseAnalyzer):
    """
    Analyzer for Statistics Management Module (Module 2).
    
    Processes unified 6-column DataFrames from StatsCollector, assigns severity,
    and registers findings. Severity contract: CRITICAL, HIGH, MEDIUM, LOW only.
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
            data: Dictionary mapping component names to DataFrames (unified 6-column schema)
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
                analyzed_results[component_key] = pd.DataFrame(columns=UNIFIED_COLUMNS + ['Severity'])
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_component(self, df: pd.DataFrame, component_key: str, severity: Severity) -> pd.DataFrame:
        """
        Generic analysis for any component using the unified schema.
        
        Adds Severity column and registers findings.
        """
        if df.empty:
            logger.warning(f"DataFrame is empty for {component_key}")
            return pd.DataFrame(columns=UNIFIED_COLUMNS + ['Severity'])
        
        if not all(col in df.columns for col in UNIFIED_COLUMNS):
            logger.warning(
                f"Missing unified columns for {component_key}. "
                f"Required: {UNIFIED_COLUMNS}, Available: {df.columns.tolist()}"
            )
            return pd.DataFrame(columns=UNIFIED_COLUMNS + ['Severity'])
        
        result_df = df[UNIFIED_COLUMNS].copy()
        result_df['Severity'] = severity.value
        
        for _, row in result_df.iterrows():
            self.add_finding(
                severity=severity,
                finding_type=row['FindingCategory'],
                description=f"{row['FindingCategory']}: {row['DatabaseName']}.{row['TableName']} ({row['ObjectName']})",
                metadata={
                    'database': row['DatabaseName'],
                    'table': row['TableName'],
                    'object': row['ObjectName'],
                    'category': row['FindingCategory'],
                    'remediation': row['RemediationDDL'],
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
