"""
Module 10 Monthly Report Analyzer

Analyzes collected monthly report data and generates formatted reports
for the Monthly Report module.
"""

import logging
import pandas as pd
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional

# ---------------------------------------------------------
# BULLETPROOF PATH ROUTING
# ---------------------------------------------------------
# Sube exactamente 1 nivel desde analyzers/mod10_monthly_analyzer.py hasta teradata-stats/
current_file_path = Path(__file__).resolve()
project_root = str(current_file_path.parent.parent)

if project_root not in sys.path:
    sys.path.insert(0, project_root)  # insert(0) fuerza a Python a buscar aquí primero

from core.base_analyzer import BaseAnalyzer

logger = logging.getLogger(__name__)


class MonthlyReportAnalyzer(BaseAnalyzer):
    """
    Analyzer for Monthly Report Module (Module 10).
    
    Analyzes data from 69 monthly report queries and formats them for presentation.
    These are predefined monthly reports that don't require complex rule-based analysis.
    """
    
    def __init__(self):
        """Initialize the Monthly Report Analyzer."""
        super().__init__(
            analyzer_name='MonthlyReportAnalyzer',
            description='Analyzes and formats monthly report data for presentation'
        )
        logger.info("Initialized MonthlyReportAnalyzer")
    
    def run(self, data: Dict[str, pd.DataFrame], config: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Run analysis on collected monthly report data in alphanumeric order.
        
        Args:
            data: Dictionary mapping report names (without .sql extension) to DataFrames
            config: Optional configuration (not used for monthly reports)
        
        Returns:
            Dictionary mapping report names to analyzed DataFrames with findings
        """
        self.clear_findings()
        analyzed_results = {}
        
        # Sort report names alphanumerically
        sorted_report_names = sorted(data.keys())
        
        # For monthly reports, we don't apply complex business rules
        # We simply validate and format the data for presentation
        for report_name in sorted_report_names:
            df = data[report_name]
            
            try:
                if df.empty:
                    logger.warning(f"Empty DataFrame for report: {report_name}")
                    analyzed_results[report_name] = df
                    continue
                
                # Basic validation - check for required columns
                if len(df.columns) == 0:
                    logger.warning(f"No columns found in report: {report_name}")
                    analyzed_results[report_name] = df
                    continue
                
                # Add metadata to the DataFrame
                df = df.copy()
                
                # No severity classification for monthly reports - these are informational
                analyzed_results[report_name] = df
                
                logger.info(f"Successfully analyzed report: {report_name} with {len(df)} rows")
                
            except Exception as e:
                logger.error(f"Failed to analyze report {report_name}: {str(e)}")
                analyzed_results[report_name] = pd.DataFrame()
        
        logger.info(f"Analyzed {len(analyzed_results)} monthly reports")
        return analyzed_results
    
    def analyze_all(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Analyze all monthly reports in batch.
        
        Args:
            data: Dictionary mapping report names to DataFrames
        
        Returns:
            Dictionary mapping report names to analyzed DataFrames
        """
        return self.run(data)
    
    def analyze_single(self, df: pd.DataFrame, report_name: str) -> pd.DataFrame:
        """
        Analyze a single monthly report.
        
        Args:
            df: DataFrame containing the report data
            report_name: Name of the report
        
        Returns:
            Analyzed DataFrame
        """
        if df.empty:
            logger.warning(f"Empty DataFrame for report: {report_name}")
            return df
        
        # Basic validation and formatting
        df = df.copy()
        
        # No complex analysis for monthly reports - just return the data
        return df
