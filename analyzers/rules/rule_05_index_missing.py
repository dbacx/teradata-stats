"""
Rule 05: Missing Index Level Statistics Detection

This rule identifies indexes (PI, SI, JI, PPI) that lack statistics,
which can prevent the optimizer from using them effectively.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

logger = logging.getLogger(__name__)


class Rule05IndexMissing(BaseStatsRule):
    """
    Rule to detect missing statistics on indexes.
    
    Secondary indexes, join indexes, and primary indexes require statistics
    for the optimizer to make informed decisions about their usage.
    """
    
    def __init__(self):
        super().__init__(
            rule_id="rule_05_index_missing",
            rule_name="Missing Index-Level Stats",
            description="Detects indexes missing statistics"
        )
        logger.info("Initialized Rule05IndexMissing")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find indexes without stats.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing indexes missing statistics
        """
        self.validate_context(context, ['stats_df'])
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            # This rule requires DBC.IndicesV data to identify all indexes
            # Since we only have stats_df, we can't identify indexes with NO stats
            # This is a limitation - in production, you'd need to extract from DBC.IndicesV
            logger.warning("Rule05IndexMissing requires DBC.IndicesV data - returning empty results")
            
            result_df = pd.DataFrame(columns=[
                'DatabaseName', 'TableName', 'ColumnName', 'recommendation', 'reason'
            ])
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule05IndexMissing analysis: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        context = {'stats_df': pd.DataFrame()}
        rule = Rule05IndexMissing()
        result = rule.analyze(context)
        print(f"Found {len(result)} indexes missing stats")
    except Exception as e:
        print(f"Error: {str(e)}")
