"""
Module 3 Space Analyzer

Analyzes collected space data and generates findings with severity levels
and DDL remediation statements for the Space Assessment module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_analyzer import BaseAnalyzer, Severity
from core.config import THRESHOLDS

logger = logging.getLogger(__name__)

COMPONENT_SEVERITY = {
    '01_Database_Space_Utilization': 'CRITICAL',
    '02_Space_Capacity_Forecast_Report': 'CRITICAL',
    '03_Suspected_Unused_Objects': 'HIGH',
    '04_Suspected_Duplicate_Objects': 'HIGH',
    '05_MVC_Opportunities_Uncompressed_Tables': 'MEDIUM',
    '06_MVC_Opportunities_Compressed_Tables': 'LOW',
    '07_Top_20_Databases_By_Used_Size': 'INFO',
    '08_Top_20_Tables_By_Size': 'INFO',
    '09_Top_20_Unused_Databases_By_Size': 'HIGH',
}

COMPONENT_LABELS = {
    '01_Database_Space_Utilization': 'Database Space Utilization',
    '02_Space_Capacity_Forecast_Report': 'Space Capacity Forecast',
    '03_Suspected_Unused_Objects': 'Suspected Unused Objects',
    '04_Suspected_Duplicate_Objects': 'Suspected Duplicate Objects',
    '05_MVC_Opportunities_Uncompressed_Tables': 'MVC Uncompressed Tables',
    '06_MVC_Opportunities_Compressed_Tables': 'MVC Compressed Tables',
    '07_Top_20_Databases_By_Used_Size': 'Top 20 Databases By Size',
    '08_Top_20_Tables_By_Size': 'Top 20 Tables By Size',
    '09_Top_20_Unused_Databases_By_Size': 'Top 20 Unused Databases',
}

DDL_COLUMNS = ['Action_SQL', 'DDL_Statement', 'DDL_Action', 'Diagnostico']


class SpaceAnalyzer(BaseAnalyzer):
    """
    Analyzer for Space Assessment Module (Module 3).
    
    Analyzes data from 9 components to identify space issues:
    1. Database Space Utilization - AMP-aware space usage
    2. Space Capacity Forecast - Growth projection to 95%
    3. Suspected Unused Objects - Tables with zero access in 30 days
    4. Suspected Duplicate Objects - Backup/temp table heuristics
    5. MVC Uncompressed Tables - Large tables with zero compression
    6. MVC Compressed Tables - Partially compressed tables with low-hanging fruit
    7. Top 20 Databases By Used Size - Executive summary
    8. Top 20 Tables By Size - Largest tables with skew
    9. Top 20 Unused Databases By Size - Cold storage candidates
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
            Dictionary mapping component names to analyzed DataFrames with Severity injected
        """
        if config is None:
            config = THRESHOLDS.copy()
        
        self.clear_findings()
        analyzed_results = {}
        
        for component_key, severity in COMPONENT_SEVERITY.items():
            df = data.get(component_key, pd.DataFrame())
            analyzed_results[component_key] = self._analyze_component(df, component_key, severity)
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_component(self, df: pd.DataFrame, component_key: str, severity: str) -> pd.DataFrame:
        """
        Generic analysis: inject Severity column and register findings.
        """
        if df.empty:
            logger.warning(f"DataFrame is empty for {component_key}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = severity
        
        label = COMPONENT_LABELS.get(component_key, component_key)
        severity_enum = getattr(Severity, severity, Severity.INFO)
        
        self.add_finding(
            severity=severity_enum,
            finding_type=component_key,
            description=f"{label}: {len(df)} hallazgos detectados",
            metadata={'component': component_key, 'row_count': len(df)}
        )
        
        return result_df
    
    def get_severity_summary(self) -> Dict[str, int]:
        """Return count of findings per severity level."""
        summary = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0, 'INFO': 0}
        for finding in self.findings:
            sev = finding.get('severity', 'INFO')
            if isinstance(sev, Severity):
                sev = sev.name
            if sev in summary:
                summary[sev] += 1
        return summary
