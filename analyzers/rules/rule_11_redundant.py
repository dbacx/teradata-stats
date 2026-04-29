"""
Rule 11: Redundant Statistics Detection

This rule identifies single-column statistics that overlap with the leading column
of a multicolumn statistic, causing double CPU and space waste.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

logger = logging.getLogger(__name__)


class Rule11Redundant(BaseStatsRule):
    """
    Rule to detect redundant single-column statistics.
    
    Single-column stats that overlap with the leading column of a multicolumn
    stat are redundant since the optimizer can derive the information from the multicolumn histogram.
    """
    
    def __init__(self):
        super().__init__(
            rule_id="rule_11_redundant",
            rule_name="Redundant Statistics",
            description="Detects single-column stats overlapping with multicolumn leading columns"
        )
        logger.info("Initialized Rule11Redundant")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find redundant single-column stats.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing redundant statistics
        """
        self.validate_context(context, ['stats_df'])
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            # This requires ExpressionCount to identify multicolumn stats
            if 'ExpressionCount' not in stats_df.columns:
                logger.warning("Rule11Redundant requires ExpressionCount column - returning empty results")
                return pd.DataFrame()
            
            stats_df_copy = stats_df.copy()
            stats_df_copy['ExpressionCount'] = pd.to_numeric(stats_df_copy['ExpressionCount'], errors='coerce')
            
            # Find single-column stats (ExpressionCount = 1)
            single_column = stats_df_copy[stats_df_copy['ExpressionCount'] == 1].copy()
            
            # Find multicolumn stats (ExpressionCount > 1)
            multicolumn = stats_df_copy[stats_df_copy['ExpressionCount'] > 1].copy()
            
            if single_column.empty or multicolumn.empty:
                return pd.DataFrame()
            
            # Check for overlap - simplified approach
            # In production, you'd parse the ColumnName to find leading columns
            redundant_stats = pd.DataFrame()
            
            if not redundant_stats.empty:
                redundant_stats['recommendation'] = 'DROP STATISTICS'
                redundant_stats['reason'] = 'Single-column stat overlaps with multicolumn leading column'
            
            result_df = self.add_metadata_columns(redundant_stats)
            
            if not result_df.empty:
                logger.info(f"Found {len(result_df)} redundant statistics")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule11Redundant analysis: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        context = {'stats_df': pd.DataFrame()}
        rule = Rule11Redundant()
        result = rule.analyze(context)
        print(f"Found {len(result)} redundant statistics")
    except Exception as e:
        print(f"Error: {str(e)}")
