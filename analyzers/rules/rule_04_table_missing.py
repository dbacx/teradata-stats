"""
Rule 04: Missing Table Level Statistics Detection

This rule identifies tables that are actively being queried but have no statistics
at all (not even a table-level COLLECT STATISTICS).
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

logger = logging.getLogger(__name__)


class Rule04TableMissing(BaseStatsRule):
    """
    Rule to detect tables with no statistics at all.
    
    Tables without any statistics force the optimizer to rely on Dynamic Amps Sampling,
    which is unreliable for large tables or skewed data, leading to suboptimal execution plans.
    """
    
    def __init__(self):
        super().__init__(
            rule_id="rule_04_table_missing",
            rule_name="Missing Table-Level Stats",
            description="Detects tables with no statistics at all"
        )
        logger.info("Initialized Rule04TableMissing")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find tables with no statistics.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing tables with no statistics
        """
        self.validate_context(context, ['stats_df'])
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            # This rule requires access to DBC.TablesV to identify all tables
            # Since we only have stats_df, we can't identify tables with NO stats
            # This is a limitation - in production, you'd need to extract from DBC.TablesV
            # For now, we return empty DataFrame with a note
            
            logger.warning("Rule04TableMissing requires DBC.TablesV data - returning empty results")
            
            result_df = pd.DataFrame(columns=[
                'DatabaseName', 'TableName', 'recommendation', 'reason'
            ])
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule04TableMissing analysis: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        context = {'stats_df': pd.DataFrame()}
        rule = Rule04TableMissing()
        result = rule.analyze(context)
        print(f"Found {len(result)} tables missing stats")
    except Exception as e:
        print(f"Error: {str(e)}")
