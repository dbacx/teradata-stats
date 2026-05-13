"""
Module 1 System Information Analyzer

Analyzes collected system information data for System Information module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_analyzer import BaseAnalyzer

logger = logging.getLogger(__name__)


class SystemInformationAnalyzer(BaseAnalyzer):
    """
    Analyzer for System Information Module (Module 1).
    
    Analyzes data from a single component:
    1. System Information - Version, release, and capacity information
    """
    
    def __init__(self):
        """Initialize System Information Analyzer."""
        super().__init__(
            analyzer_name='SystemInformationAnalyzer',
            description='Analyzes Teradata system information data'
        )
        logger.info("Initialized SystemInformationAnalyzer")
    
    def run(self, data: pd.DataFrame, config: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Run analysis on collected system information data.
        
        Args:
            data: DataFrame containing system information
            config: Optional configuration (not used in this module)
        
        Returns:
            DataFrame containing raw system information for visualization
        """
        if data.empty:
            logger.warning("DataFrame is empty for system information")
            return data
        
        required_columns = ['Metrica', 'Valor']
        if not all(col in data.columns for col in required_columns):
            logger.warning(f"Missing required columns for system information. Required: {required_columns}, Available: {data.columns.tolist()}")
            return data
        
        # No business logic, severity calculation, or DDL generation
        # Simply return the raw DataFrame for visualization
        logger.info(f"Analysis complete. System information contains {len(data)} rows")
        return data
