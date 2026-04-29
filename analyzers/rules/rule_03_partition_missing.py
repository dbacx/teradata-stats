"""
Rule 03: Missing PARTITION Level Statistics Detection

This rule identifies PPI (Partition Primary Index) tables that are missing
statistics on the PARTITION column, which is critical for efficient Partition Elimination.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

logger = logging.getLogger(__name__)


class Rule03PartitionMissing(BaseStatsRule):
    """
    Rule to detect missing PARTITION statistics on PPI tables.
    
    PPI tables require statistics on the PARTITION column for the optimizer
    to perform efficient partition elimination. Without these stats, the optimizer
    may perform full table scans instead of accessing only relevant partitions.
    """
    
    def __init__(self):
        super().__init__(
            rule_id="rule_03_partition_missing",
            rule_name="Missing PARTITION Stats",
            description="Detects PPI tables missing PARTITION column statistics"
        )
        logger.info("Initialized Rule03PartitionMissing")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find PPI tables without PARTITION stats.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing PPI tables missing PARTITION stats
        """
        self.validate_context(context, ['stats_df'])
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            # Group by DatabaseName and TableName to identify tables
            # Check if any table has stats on PARTITION column
            table_has_partition_stats = (
                stats_df[stats_df['ColumnName'] == 'PARTITION']
                .groupby(['DatabaseName', 'TableName'])
                .size()
                .reset_index(name='has_partition_stats')
            )
            
            # Get all unique tables from stats
            all_tables = (
                stats_df[['DatabaseName', 'TableName']]
                .drop_duplicates()
            )
            
            # Identify tables WITHOUT partition stats
            # This is a simplified approach - in production, you'd need to check
            # DBC.IndexConstraints to identify actual PPI tables
            # For now, we flag tables that don't have PARTITION stats
            tables_without_partition = all_tables.merge(
                table_has_partition_stats,
                on=['DatabaseName', 'TableName'],
                how='left'
            )
            
            missing_partition = tables_without_partition[
                tables_without_partition['has_partition_stats'].isna()
            ].copy()
            
            # Add analysis-specific columns
            missing_partition['recommendation'] = 'COLLECT STATISTICS COLUMN (PARTITION)'
            missing_partition['reason'] = 'PPI table missing PARTITION column statistics'
            
            result_df = self.add_metadata_columns(missing_partition)
            
            if not result_df.empty:
                logger.info(f"Found {len(result_df)} tables potentially missing PARTITION stats")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule03PartitionMissing analysis: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        sample_stats = pd.DataFrame({
            'DatabaseName': ['DB1', 'DB1', 'DB2'],
            'TableName': ['Table1', 'Table2', 'Table1'],
            'ColumnName': ['Col1', 'PARTITION', 'Col1'],
            'StatsName': ['Stats1', 'Stats2', 'Stats3']
        })
        
        context = {'stats_df': sample_stats}
        rule = Rule03PartitionMissing()
        result = rule.analyze(context)
        print(f"Found {len(result)} tables missing PARTITION stats")
    except Exception as e:
        print(f"Error: {str(e)}")
