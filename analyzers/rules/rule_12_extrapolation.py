"""
Rule 12: Extrapolation Risk Detection

This rule identifies massive discrepancies between Stats RowCount and physical table size,
indicating extrapolation risk where the optimizer may make severe underestimations.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

logger = logging.getLogger(__name__)


class Rule12Extrapolation(BaseStatsRule):
    """
    Rule to detect extrapolation risk due to RowCount/Size mismatch.
    
    Linear extrapolation can fail dramatically if data distribution changes,
    leading to severe underestimations and suboptimal plans.
    """
    
    def __init__(self, min_rowcount: int = 1000, min_size_gb: float = 1.0):
        super().__init__(
            rule_id="rule_12_extrapolation",
            rule_name="Extrapolation Risk",
            description="Detects massive RowCount vs TableSize discrepancies"
        )
        self.min_rowcount = min_rowcount
        self.min_size_gb = min_size_gb
        logger.info(f"Initialized Rule12Extrapolation with RowCount<{min_rowcount}, Size>{min_size_gb}GB")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find extrapolation risk.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing tables with extrapolation risk
        """
        self.validate_context(context, ['stats_df'])
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            stats_df_copy = stats_df.copy()
            stats_df_copy['RowCount'] = pd.to_numeric(stats_df_copy['RowCount'], errors='coerce')
            stats_df_copy['TableSizeGB'] = pd.to_numeric(stats_df_copy['TableSizeGB'], errors='coerce')
            
            # Find stats with very low RowCount but large TableSize
            extrapolation_mask = (
                (stats_df_copy['RowCount'] < self.min_rowcount) &
                (stats_df_copy['TableSizeGB'] > self.min_size_gb)
            )
            
            extrapolation_risk = stats_df_copy[extrapolation_mask].copy()
            
            if not extrapolation_risk.empty:
                extrapolation_risk['recommendation'] = 'COLLECT STATISTICS'
                extrapolation_risk['reason'] = f'Massive discrepancy: RowCount<{self.min_rowcount} but Size>{self.min_size_gb}GB'
            
            result_df = self.add_metadata_columns(extrapolation_risk)
            
            if not result_df.empty:
                logger.info(f"Found {len(result_df)} tables with extrapolation risk")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule12Extrapolation analysis: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        sample_stats = pd.DataFrame({
            'DatabaseName': ['DB1', 'DB2'],
            'TableName': ['Table1', 'Table2'],
            'ColumnName': ['Col1', 'Col2'],
            'RowCount': [500, 1000000],
            'TableSizeGB': [5.0, 0.5]
        })
        
        context = {'stats_df': sample_stats}
        rule = Rule12Extrapolation()
        result = rule.analyze(context)
        print(f"Found {len(result)} tables with extrapolation risk")
    except Exception as e:
        print(f"Error: {str(e)}")
