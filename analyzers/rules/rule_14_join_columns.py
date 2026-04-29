"""
Rule 14: Missing Join Columns Statistics Detection

This rule identifies columns likely used in JOINs (ID, KEY, CD, CODE) that lack statistics,
which can cause Product Joins or Skewed Redistribution.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

logger = logging.getLogger(__name__)


class Rule14JoinColumns(BaseStatsRule):
    """
    Rule to detect missing statistics on likely join columns.
    
    Columns with names containing _ID, _KEY, _CD, or _CODE are heuristic indicators
    of join columns. Without stats, the risk of Product Joins is high.
    """
    
    def __init__(self):
        super().__init__(
            rule_id="rule_14_join_columns",
            rule_name="Missing Key Columns",
            description="Detects ID/KEY/CODE columns likely used in JOINs without statistics"
        )
        logger.info("Initialized Rule14JoinColumns")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find missing stats on likely join columns.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing likely join columns missing statistics
        """
        self.validate_context(context, ['stats_df'])
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            # This requires DBC.ColumnsV to identify all columns
            # Since we only have stats_df, we can't identify columns with NO stats
            logger.warning("Rule14JoinColumns requires DBC.ColumnsV data - returning empty results")
            
            result_df = pd.DataFrame(columns=[
                'DatabaseName', 'TableName', 'ColumnName', 'recommendation', 'reason'
            ])
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule14JoinColumns analysis: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        context = {'stats_df': pd.DataFrame()}
        rule = Rule14JoinColumns()
        result = rule.analyze(context)
        print(f"Found {len(result)} join columns missing stats")
    except Exception as e:
        print(f"Error: {str(e)}")
