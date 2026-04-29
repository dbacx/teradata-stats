"""
Rule 13: Hardcoded Sample Detection

This rule identifies statistics with hardcoded USING SAMPLE values that override
System Auto-Stats intelligence, blocking dynamic decision-making.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

logger = logging.getLogger(__name__)


class Rule13Analyze(BaseStatsRule):
    """
    Rule to detect hardcoded sample statistics.
    
    Hardcoded SAMPLE values prevent the system from making intelligent
    decisions about when and how to collect statistics automatically.
    """
    
    def __init__(self):
        super().__init__(
            rule_id="rule_13_analyze",
            rule_name="Hardcoded Samples",
            description="Detects stats with forced USING SAMPLE overriding Auto-Stats"
        )
        logger.info("Initialized Rule13Analyze")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find hardcoded sample stats.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing hardcoded sample statistics
        """
        self.validate_context(context, ['stats_df'])
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            if 'SampleSizePct' not in stats_df.columns:
                logger.warning("Rule13Analyze requires SampleSizePct column - returning empty results")
                return pd.DataFrame()
            
            stats_df_copy = stats_df.copy()
            stats_df_copy['SampleSizePct'] = pd.to_numeric(stats_df_copy['SampleSizePct'], errors='coerce')
            
            # Find stats with hardcoded sample (0 < SampleSizePct < 100)
            hardcoded_sample_mask = (
                (stats_df_copy['SampleSizePct'] > 0) &
                (stats_df_copy['SampleSizePct'] < 100)
            )
            
            hardcoded_sample = stats_df_copy[hardcoded_sample_mask].copy()
            
            if not hardcoded_sample.empty:
                hardcoded_sample['recommendation'] = 'REVIEW SAMPLE CONFIGURATION'
                hardcoded_sample['reason'] = 'Hardcoded USING SAMPLE overrides System Auto-Stats intelligence'
            
            result_df = self.add_metadata_columns(hardcoded_sample)
            
            if not result_df.empty:
                logger.info(f"Found {len(result_df)} hardcoded sample statistics")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule13Analyze analysis: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        sample_stats = pd.DataFrame({
            'DatabaseName': ['DB1', 'DB2'],
            'TableName': ['Table1', 'Table2'],
            'ColumnName': ['Col1', 'Col2'],
            'SampleSizePct': [5.0, 100]
        })
        
        context = {'stats_df': sample_stats}
        rule = Rule13Analyze()
        result = rule.analyze(context)
        print(f"Found {len(result)} hardcoded sample statistics")
    except Exception as e:
        print(f"Error: {str(e)}")
