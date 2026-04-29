"""
Rule 08: Multicolumn Statistics Detection

This rule identifies multicolumn statistics where the combined column length
exceeds the MaxValueLength, causing truncation and loss of precision.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

logger = logging.getLogger(__name__)


class Rule08Multicolumn(BaseStatsRule):
    """
    Rule to detect multicolumn statistics with excessive combined length.
    
    Multicolumn stats can be truncated if the combined length exceeds
    MaxValueLength, losing precision in selectivity estimation.
    """
    
    def __init__(self, max_value_length_threshold: int = 25):
        super().__init__(
            rule_id="rule_08_multicolumn",
            rule_name="Multicolumn MaxValueLength",
            description="Detects multicolumn stats exceeding MaxValueLength"
        )
        self.max_value_length_threshold = max_value_length_threshold
        logger.info(f"Initialized Rule08Multicolumn with threshold {max_value_length_threshold}")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find multicolumn stats with excessive length.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing multicolumn stats with excessive length
        """
        self.validate_context(context, ['stats_df'])
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            # This requires ExpressionCount and MaxValueLength columns
            # These may not be in the current schema - checking availability
            if 'ExpressionCount' not in stats_df.columns or 'MaxValueLength' not in stats_df.columns:
                logger.warning("Rule08Multicolumn requires ExpressionCount and MaxValueLength columns - returning empty results")
                return pd.DataFrame()
            
            stats_df_copy = stats_df.copy()
            stats_df_copy['ExpressionCount'] = pd.to_numeric(stats_df_copy['ExpressionCount'], errors='coerce')
            stats_df_copy['MaxValueLength'] = pd.to_numeric(stats_df_copy['MaxValueLength'], errors='coerce')
            
            # Find multicolumn stats (ExpressionCount > 1) with low MaxValueLength
            multicolumn_mask = (
                (stats_df_copy['ExpressionCount'] > 1) &
                (stats_df_copy['MaxValueLength'] < self.max_value_length_threshold)
            )
            
            multicolumn_stats = stats_df_copy[multicolumn_mask].copy()
            
            if not multicolumn_stats.empty:
                multicolumn_stats['recommendation'] = 'REVIEW MULTICOLUMN STATS'
                multicolumn_stats['reason'] = f'Multicolumn stats with MaxValueLength < {self.max_value_length_threshold} may be truncated'
            
            result_df = self.add_metadata_columns(multicolumn_stats)
            
            if not result_df.empty:
                logger.info(f"Found {len(result_df)} multicolumn stats with excessive length")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule08Multicolumn analysis: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        sample_stats = pd.DataFrame({
            'DatabaseName': ['DB1', 'DB2'],
            'TableName': ['Table1', 'Table2'],
            'ColumnName': ['Col1,Col2', 'Col3'],
            'ExpressionCount': [2, 1],
            'MaxValueLength': [20, 50]
        })
        
        context = {'stats_df': sample_stats}
        rule = Rule08Multicolumn()
        result = rule.analyze(context)
        print(f"Found {len(result)} multicolumn stats with excessive length")
    except Exception as e:
        print(f"Error: {str(e)}")
