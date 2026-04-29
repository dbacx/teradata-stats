"""
Base Rule Abstract Class for Statistics Analysis

This module defines the abstract base class that all statistics analysis rules
must inherit from, implementing the Strategy Pattern for maximum scalability.
"""

import logging
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)


class BaseStatsRule(ABC):
    """
    Abstract base class for statistics analysis rules.
    
    All specific rule implementations must inherit from this class and
    implement the analyze method. This ensures consistency across all
    rules and enables the Strategy Pattern implementation.
    """
    
    def __init__(self, rule_id: str, rule_name: str, description: str):
        """
        Initialize the base rule with metadata.
        
        Args:
            rule_id: Unique identifier for the rule (e.g., "rule_01_unused")
            rule_name: Human-readable name for the rule (e.g., "Unused Objects")
            description: Detailed description of what the rule analyzes
        """
        self.rule_id = rule_id
        self.rule_name = rule_name
        self.description = description
        self.analysis_date = datetime.now()
        
        logger.debug(f"Initialized rule: {self.rule_id} - {self.rule_name}")
    
    @abstractmethod
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data based on the rule logic.
        
        This method must be implemented by each concrete rule class.
        It should use vectorized pandas operations for optimal performance.
        
        Args:
            context: Dictionary containing DataFrames needed for analysis.
                    Expected keys include 'stats_df', 'usage_df', etc.
        
        Returns:
            DataFrame containing the analysis results with rule-specific columns.
                    Should include metadata columns like 'rule_id', 'analysis_date'.
        
        Raises:
            ValueError: If required context keys are missing or invalid
            Exception: If analysis fails for any reason
        """
        pass
    
    def validate_context(self, context: Dict[str, pd.DataFrame], required_keys: list) -> None:
        """
        Validate that the context contains all required DataFrames.
        
        Args:
            context: Dictionary containing DataFrames for analysis
            required_keys: List of required keys that must be present in context
        
        Raises:
            ValueError: If any required key is missing or DataFrame is empty
        """
        missing_keys = [key for key in required_keys if key not in context]
        if missing_keys:
            raise ValueError(f"Missing required context keys: {missing_keys}")
        
        empty_dfs = [key for key in required_keys if context[key].empty]
        if empty_dfs:
            raise ValueError(f"Empty DataFrames for required keys: {empty_dfs}")
    
    def add_metadata_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add standard metadata columns to the result DataFrame.
        
        Args:
            df: DataFrame to add metadata to
        
        Returns:
            DataFrame with added metadata columns
        """
        df = df.copy()
        df['rule_id'] = self.rule_id
        df['rule_name'] = self.rule_name
        df['analysis_date'] = self.analysis_date
        
        return df
    
    def log_analysis_start(self, context: Dict[str, pd.DataFrame]) -> None:
        """Log the start of analysis for this rule."""
        logger.info(f"Starting analysis for rule: {self.rule_id} - {self.rule_name}")
        
        # Log context information
        for key, df in context.items():
            if isinstance(df, pd.DataFrame):
                logger.debug(f"Context '{key}': {len(df)} rows, {len(df.columns)} columns")
    
    def log_analysis_end(self, result_df: pd.DataFrame) -> None:
        """Log the end of analysis for this rule."""
        result_count = len(result_df)
        logger.info(f"Completed analysis for rule: {self.rule_id} - {result_count} findings")
        
        if result_count > 0:
            # Log summary statistics if available
            if 'DatabaseName' in result_df.columns:
                db_count = result_df['DatabaseName'].nunique()
                logger.debug(f"Findings span {db_count} databases")
            
            if 'TableName' in result_df.columns:
                table_count = result_df['TableName'].nunique()
                logger.debug(f"Findings span {table_count} tables")
    
    def get_rule_info(self) -> Dict[str, Any]:
        """
        Get information about this rule.
        
        Returns:
            Dictionary containing rule metadata
        """
        return {
            'rule_id': self.rule_id,
            'rule_name': self.rule_name,
            'description': self.description,
            'analysis_date': self.analysis_date
        }
    
    def __str__(self) -> str:
        """String representation of the rule."""
        return f"{self.rule_id}: {self.rule_name}"
    
    def __repr__(self) -> str:
        """Detailed string representation of the rule."""
        return f"BaseStatsRule(rule_id='{self.rule_id}', rule_name='{self.rule_name}')"
