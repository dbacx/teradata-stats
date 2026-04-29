"""
Base Analyzer Abstract Class for Data Analysis

This module defines the abstract base class that all data analyzers
must inherit from, providing a consistent interface for data analysis
and finding registration.
"""

import logging
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
from enum import Enum

# Configure logging
logger = logging.getLogger(__name__)


class Severity(Enum):
    """Severity levels for findings."""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class BaseAnalyzer(ABC):
    """
    Abstract base class for data analyzers.
    
    All specific analyzer implementations must inherit from this class and
    implement the run method. This ensures consistency across all
    analyzers and enables finding registration with severity levels.
    """
    
    def __init__(self, analyzer_name: str, description: str):
        """
        Initialize the base analyzer with metadata.
        
        Args:
            analyzer_name: Name of the analyzer (e.g., "StatsAnalyzer")
            description: Detailed description of what the analyzer does
        """
        self.analyzer_name = analyzer_name
        self.description = description
        self.analysis_date = datetime.now()
        self.findings: List[Dict[str, Any]] = []
        
        logger.debug(f"Initialized analyzer: {self.analyzer_name}")
    
    @abstractmethod
    def run(self, data: pd.DataFrame, config: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Run the analysis on the provided data.
        
        This method must be implemented by each concrete analyzer class.
        It should evaluate the data against configuration rules and register findings.
        
        Args:
            data: DataFrame containing the data to analyze
            config: Optional configuration dictionary with thresholds and rules
        
        Returns:
            DataFrame containing the analysis results with findings
        
        Raises:
            ValueError: If required configuration is missing
            Exception: If analysis fails for any reason
        """
        pass
    
    def add_finding(self, severity: Severity, finding_type: str, 
                    description: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Register a finding with the specified severity.
        
        Args:
            severity: Severity level (CRITICAL, HIGH, MEDIUM, LOW, INFO)
            finding_type: Type of finding (e.g., "STALE_STATS", "MISSING_STATS")
            description: Human-readable description of the finding
            metadata: Optional dictionary with additional finding details
        """
        finding = {
            'severity': severity.value,
            'finding_type': finding_type,
            'description': description,
            'metadata': metadata or {},
            'analyzer_name': self.analyzer_name,
            'analysis_date': self.analysis_date
        }
        
        self.findings.append(finding)
        logger.debug(f"Added finding: {severity.value} - {finding_type}")
    
    def get_findings(self) -> List[Dict[str, Any]]:
        """
        Get all registered findings.
        
        Returns:
            List of finding dictionaries
        """
        return self.findings
    
    def get_findings_by_severity(self, severity: Severity) -> List[Dict[str, Any]]:
        """
        Get findings filtered by severity.
        
        Args:
            severity: Severity level to filter by
        
        Returns:
            List of findings with the specified severity
        """
        return [f for f in self.findings if f['severity'] == severity.value]
    
    def get_finding_count(self) -> int:
        """
        Get total count of findings.
        
        Returns:
            Total number of findings
        """
        return len(self.findings)
    
    def get_severity_summary(self) -> Dict[str, int]:
        """
        Get summary of findings by severity.
        
        Returns:
            Dictionary with counts per severity level
        """
        summary = {severity.value: 0 for severity in Severity}
        
        for finding in self.findings:
            severity = finding['severity']
            summary[severity] = summary.get(severity, 0) + 1
        
        return summary
    
    def clear_findings(self) -> None:
        """Clear all registered findings."""
        self.findings.clear()
        logger.debug(f"Cleared all findings for analyzer: {self.analyzer_name}")
    
    def generate_ddl_collect(self, database: str, table: str, column: str) -> str:
        """
        Generate COLLECT STATISTICS DDL statement.
        
        Args:
            database: Database name
            table: Table name
            column: Column name (or list of columns)
        
        Returns:
            DDL statement string
        """
        if isinstance(column, list):
            column_str = ", ".join(column)
        else:
            column_str = column
        
        return f"COLLECT STATISTICS ON {database}.{table} COLUMN ({column_str});"
    
    def generate_ddl_drop(self, database: str, table: str, column: str) -> str:
        """
        Generate DROP STATISTICS DDL statement.
        
        Args:
            database: Database name
            table: Table name
            column: Column name
        
        Returns:
            DDL statement string
        """
        return f"DROP STATISTICS ON {database}.{table} COLUMN ({column});"
    
    def validate_config(self, config: Optional[Dict[str, Any]], required_keys: List[str]) -> None:
        """
        Validate that configuration contains all required keys.
        
        Args:
            config: Configuration dictionary
            required_keys: List of required configuration keys
        
        Raises:
            ValueError: If any required key is missing
        """
        if config is None:
            raise ValueError("Configuration is required but was None")
        
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")
    
    def log_analysis_start(self, data: pd.DataFrame) -> None:
        """Log the start of analysis."""
        logger.info(f"Starting analysis with {self.analyzer_name}")
        logger.debug(f"Data shape: {data.shape}")
    
    def log_analysis_end(self) -> None:
        """Log the end of analysis."""
        finding_count = len(self.findings)
        severity_summary = self.get_severity_summary()
        logger.info(f"Completed analysis with {self.analyzer_name}. Total findings: {finding_count}")
        logger.debug(f"Severity summary: {severity_summary}")
    
    def get_analyzer_info(self) -> Dict[str, Any]:
        """
        Get information about this analyzer.
        
        Returns:
            Dictionary containing analyzer metadata
        """
        return {
            'analyzer_name': self.analyzer_name,
            'description': self.description,
            'analysis_date': self.analysis_date,
            'total_findings': len(self.findings),
            'severity_summary': self.get_severity_summary()
        }
    
    def __str__(self) -> str:
        """String representation of the analyzer."""
        return f"BaseAnalyzer(name='{self.analyzer_name}', findings={len(self.findings)})"
    
    def __repr__(self) -> str:
        """Detailed string representation of the analyzer."""
        return f"BaseAnalyzer(analyzer_name='{self.analyzer_name}', description='{self.description}', findings={len(self.findings)})"
