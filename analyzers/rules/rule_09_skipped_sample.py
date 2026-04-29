"""
Rule 09: Skipped and Sample Statistics Detection

This rule identifies statistics that are being skipped by the system or using sample,
which may indicate threshold configuration issues or imprecision for skewed data.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

logger = logging.getLogger(__name__)


class Rule09SkippedSample(BaseStatsRule):
    """
    Rule to detect skipped or sampled statistics.
    
    System-determined skipping or excessive sampling can mask data skew
    and lead to suboptimal execution plans.
    """
    
    def __init__(self):
        super().__init__(
            rule_id="rule_09_skipped_sample",
            rule_name="Skipped and Sample Stats",
            description="Detects statistics being skipped or using sample"
        )
        logger.info("Initialized Rule09SkippedSample")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find skipped or sampled stats.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing skipped or sampled statistics
        """
        self.validate_context(context, ['stats_df'])
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            stats_df_copy = stats_df.copy()
            
            # Check for SampleSizePct and StatsSkipCount columns
            has_sample = 'SampleSizePct' in stats_df_copy.columns
            has_skip = 'StatsSkipCount' in stats_df_copy.columns
            
            if has_sample:
                stats_df_copy['SampleSizePct'] = pd.to_numeric(stats_df_copy['SampleSizePct'], errors='coerce')
            
            if has_skip:
                stats_df_copy['StatsSkipCount'] = pd.to_numeric(stats_df_copy['StatsSkipCount'], errors='coerce')
            
            # Find stats using sample (0 < SampleSizePct < 100) or skipped (StatsSkipCount > 0)
            conditions = []
            if has_sample:
                conditions.append((stats_df_copy['SampleSizePct'] > 0) & (stats_df_copy['SampleSizePct'] < 100))
            if has_skip:
                conditions.append(stats_df_copy['StatsSkipCount'] > 0)
            
            if conditions:
                skipped_sample_mask = conditions[0]
                for cond in conditions[1:]:
                    skipped_sample_mask |= cond
            else:
                logger.warning("Rule09SkippedSample requires SampleSizePct or StatsSkipCount columns - returning empty results")
                return pd.DataFrame()
            
            skipped_sample_stats = stats_df_copy[skipped_sample_mask].copy()
            
            if not skipped_sample_stats.empty:
                skipped_sample_stats['recommendation'] = 'REVIEW THRESHOLD CONFIGURATION'
                skipped_sample_stats['reason'] = 'Statistics being skipped or using sample - may indicate threshold issues'
            
            result_df = self.add_metadata_columns(skipped_sample_stats)
            
            if not result_df.empty:
                logger.info(f"Found {len(result_df)} skipped or sampled statistics")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule09SkippedSample analysis: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        sample_stats = pd.DataFrame({
            'DatabaseName': ['DB1', 'DB2', 'DB3'],
            'TableName': ['Table1', 'Table2', 'Table3'],
            'ColumnName': ['Col1', 'Col2', 'Col3'],
            'SampleSizePct': [5.0, 100, 0],
            'StatsSkipCount': [0, 0, 5]
        })
        
        context = {'stats_df': sample_stats}
        rule = Rule09SkippedSample()
        result = rule.analyze(context)
        print(f"Found {len(result)} skipped or sampled statistics")
    except Exception as e:
        print(f"Error: {str(e)}")
