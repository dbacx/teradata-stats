"""
Rule 07: Zero Statistics Detection

This rule identifies statistics showing RowCount = 0 on tables that actually contain data,
which is dangerous as the optimizer may choose Product Join plans for tables with millions of rows.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

logger = logging.getLogger(__name__)


class Rule07ZeroStats(BaseStatsRule):
    """
    Rule to detect statistics with zero row count on populated tables.
    
    Zero row count statistics are dangerous because the optimizer may believe
    the table is empty and choose Product Join plans, which can hang the system
    if the table actually contains millions of rows.
    """
    
    def __init__(self):
        super().__init__(
            rule_id="rule_07_zero_stats",
            rule_name="Zero Count Statistics",
            description="Detects stats with Zero Count on populated tables"
        )
        logger.info("Initialized Rule07ZeroStats")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find zero row count stats on populated tables.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing zero row count statistics
        """
        self.validate_context(context, ['stats_df'])
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            # Convert RowCount to numeric
            stats_df_copy = stats_df.copy()
            stats_df_copy['RowCount'] = pd.to_numeric(stats_df_copy['RowCount'], errors='coerce')
            stats_df_copy['TableSizeGB'] = pd.to_numeric(stats_df_copy['TableSizeGB'], errors='coerce')
            
            # Find stats with RowCount = 0 but TableSizeGB > 0 (table has data)
            zero_stats_mask = (
                (stats_df_copy['RowCount'] == 0) &
                (stats_df_copy['TableSizeGB'] > 0)
            )
            
            zero_stats = stats_df_copy[zero_stats_mask].copy()
            
            if not zero_stats.empty:
                zero_stats['recommendation'] = 'COLLECT STATISTICS'
                zero_stats['reason'] = 'Zero RowCount on populated table - dangerous for optimizer'
            
            result_df = self.add_metadata_columns(zero_stats)
            
            if not result_df.empty:
                logger.info(f"Found {len(result_df)} zero count statistics")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule07ZeroStats analysis: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        sample_stats = pd.DataFrame({
            'DatabaseName': ['DB1', 'DB2'],
            'TableName': ['Table1', 'Table2'],
            'ColumnName': ['Col1', 'Col2'],
            'RowCount': [0, 100],
            'TableSizeGB': [50.0, 10.0]
        })
        
        context = {'stats_df': sample_stats}
        rule = Rule07ZeroStats()
        result = rule.analyze(context)
        print(f"Found {len(result)} zero count statistics")
    except Exception as e:
        print(f"Error: {str(e)}")
