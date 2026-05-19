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
    '01_CDS_Report': 'CRITICAL',
    '02_Space_Capacity_Forecast': 'CRITICAL',
    '03_Suspected_Unused_Objects': 'HIGH',
    '04_Suspected_Duplicate_Objects': 'HIGH',
    '05_MVC_Opportunities_Uncompressed_Tables': 'MEDIUM',
    '06_MVC_Opportunities_Compressed_Tables': 'LOW',
    '07_Top_20_Databases_By_Used_Size': 'INFO',
    '08_Top_20_Tables_By_Size': 'INFO',
    '09_Top_20_Unused_Databases_By_Size': 'HIGH',
    '10_Monthly_Capacity_ Snapshot': 'MEDIUM',
    '11_Database_Space_Utilization': 'CRITICAL',
}

COMPONENT_LABELS = {
    '01_CDS_Report': 'CDS Report',
    '02_Space_Capacity_Forecast': 'Space Capacity Forecast',
    '03_Suspected_Unused_Objects': 'Suspected Unused Objects',
    '04_Suspected_Duplicate_Objects': 'Suspected Duplicate Objects',
    '05_MVC_Opportunities_Uncompressed_Tables': 'MVC Uncompressed Tables',
    '06_MVC_Opportunities_Compressed_Tables': 'MVC Compressed Tables',
    '07_Top_20_Databases_By_Used_Size': 'Top 20 Databases By Size',
    '08_Top_20_Tables_By_Size': 'Top 20 Tables By Size',
    '09_Top_20_Unused_Databases_By_Size': 'Top 20 Unused Databases',
    '10_Monthly_Capacity_ Snapshot': 'Monthly Capacity Snapshot',
    '11_Database_Space_Utilization': 'Database Space Utilization',
}

DDL_COLUMNS = ['Action_SQL', 'DDL_Statement', 'DDL_Action', 'Diagnostico', 'RemediationDDL']


class SpaceAnalyzer(BaseAnalyzer):
    """
    Analyzer for Space Assessment Module (Module 3).
    
    Analyzes data from 11 components to identify space issues,
    generates DDL remediation statements, and enforces CDS validation
    (CDS consumed must be <= Perm capacity).
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
        Analyze a component: inject Severity, generate DDL, validate CDS rule.
        """
        if df.empty:
            logger.warning(f"DataFrame is empty for {component_key}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = severity
        
        if component_key == '01_CDS_Report':
            result_df = self._validate_cds(result_df)
        elif component_key == '03_Suspected_Unused_Objects':
            result_df = self._generate_drop_ddl(result_df)
        elif component_key == '04_Suspected_Duplicate_Objects':
            result_df = self._generate_duplicate_ddl(result_df)
        elif component_key in ('05_MVC_Opportunities_Uncompressed_Tables',
                               '06_MVC_Opportunities_Compressed_Tables'):
            result_df = self._generate_compress_ddl(result_df)
        
        label = COMPONENT_LABELS.get(component_key, component_key)
        severity_enum = getattr(Severity, severity, Severity.INFO)
        
        self.add_finding(
            severity=severity_enum,
            finding_type=component_key,
            description=f"{label}: {len(df)} hallazgos detectados",
            metadata={'component': component_key, 'row_count': len(df)}
        )
        
        return result_df
    
    def _validate_cds(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        CDS validation: CDS consumed must be <= Perm capacity.
        Adds CDS_Validation column.
        """
        if 'Parameter' in df.columns and 'Value' in df.columns:
            params = dict(zip(df['Parameter'].str.strip(), df['Value']))
            cds_consumed = params.get('04. CDS Consumed (TB)', 0)
            current_perm = params.get('01. CurrentPerm (TB)', 0)
            try:
                cds_consumed = float(cds_consumed)
                current_perm = float(current_perm)
            except (ValueError, TypeError):
                cds_consumed = 0.0
                current_perm = 0.0
            if cds_consumed > current_perm and current_perm > 0:
                logger.warning(f"CDS validation failed: CDS({cds_consumed} TB) > Perm({current_perm} TB)")
                df['CDS_Validation'] = 'FAIL: CDS > Perm'
            else:
                df['CDS_Validation'] = 'PASS'
        return df
    
    def _generate_drop_ddl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate DROP TABLE DDL for unused objects."""
        if 'DatabaseName' in df.columns and 'TableName' in df.columns:
            df['DDL_Statement'] = 'DROP TABLE ' + df['DatabaseName'].astype(str) + '.' + df['TableName'].astype(str) + ';'
            df['DDL_Action'] = 'DROP TABLE'
        return df
    
    def _generate_duplicate_ddl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate DROP TABLE DDL for suspected duplicate objects."""
        if 'DatabaseName' in df.columns and 'TableName' in df.columns:
            df['DDL_Statement'] = 'DROP TABLE ' + df['DatabaseName'].astype(str) + '.' + df['TableName'].astype(str) + ';'
            df['DDL_Action'] = 'DROP TABLE'
        return df
    
    def _generate_compress_ddl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate ALTER TABLE ADD COMPRESS DDL for MVC opportunities."""
        if 'DatabaseName' in df.columns and 'TableName' in df.columns:
            if 'ColumnName' in df.columns:
                df['DDL_Statement'] = (
                    'ALTER TABLE ' + df['DatabaseName'].astype(str) + '.' +
                    df['TableName'].astype(str) + ' ADD ' +
                    df['ColumnName'].astype(str) + ' COMPRESS;'
                )
            else:
                df['DDL_Statement'] = (
                    'ALTER TABLE ' + df['DatabaseName'].astype(str) + '.' +
                    df['TableName'].astype(str) + ' ADD COMPRESS;'
                )
            df['DDL_Action'] = 'ADD COMPRESS'
        return df
    
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
