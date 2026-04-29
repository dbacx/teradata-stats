"""
Rule 10: Missing DBC/PDCR Statistics Detection

This rule identifies missing statistics on system tables (DBC, PDCRDATA),
which are critical for monitoring and query parsing performance.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

logger = logging.getLogger(__name__)


class Rule10DBCMissing(BaseStatsRule):
    """
    Rule to detect missing statistics on DBC and PDCRDATA tables.
    
    System tables also require statistics for optimal performance of
    monitoring queries and user query parsing.
    """
    
    def __init__(self):
        super().__init__(
            rule_id="rule_10_dbc_missing",
            rule_name="Missing DBC/PDCR Stats",
            description="Detects missing stats in DBC and PDCRDATA system tables"
        )
        logger.info("Initialized Rule10DBCMissing")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find missing DBC/PDCR stats.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing DBC/PDCR tables missing statistics
        """
        self.validate_context(context, ['stats_df'])
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            # This rule requires DBC.TablesV to identify all DBC/PDCR tables
            # Since we only have stats_df, we can't identify tables with NO stats
            logger.warning("Rule10DBCMissing requires DBC.TablesV data - returning empty results")
            
            result_df = pd.DataFrame(columns=[
                'DatabaseName', 'TableName', 'recommendation', 'reason'
            ])
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule10DBCMissing analysis: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        context = {'stats_df': pd.DataFrame()}
        rule = Rule10DBCMissing()
        result = rule.analyze(context)
        print(f"Found {len(result)} DBC/PDCR tables missing stats")
    except Exception as e:
        print(f"Error: {str(e)}")
