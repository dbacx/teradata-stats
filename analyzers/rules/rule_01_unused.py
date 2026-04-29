"""
Rule 01: Unused Objects Detection

This rule identifies statistics on tables that have not been accessed recently,
indicating potentially unnecessary statistics that could be dropped to save
space and improve maintenance efficiency.
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

# Configure logging
logger = logging.getLogger(__name__)


class Rule01Unused(BaseStatsRule):
    """
    Rule to detect unused statistics based on object access patterns.
    
    This rule analyzes statistics on tables that have not been accessed
    within a specified time threshold, indicating they may be candidates
    for cleanup.
    """
    
    def __init__(self, days_threshold: int = 30):
        """
        Initialize the unused objects rule.
        
        Args:
            days_threshold: Number of days without access to consider as unused (default: 30)
        """
        super().__init__(
            rule_id="rule_01_unused",
            rule_name="Unused Objects",
            description="Detects statistics on tables that have not been accessed recently"
        )
        
        self.days_threshold = days_threshold
        logger.info(f"Initialized Rule01Unused with {days_threshold} days threshold")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find unused objects.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df', 'usage_df'
        
        Returns:
            DataFrame containing unused statistics with metadata columns
        """
        # Validate context
        self.validate_context(context, ['stats_df'])
        
        stats_df = context['stats_df']
        usage_df = context.get('usage_df', pd.DataFrame())
        
        self.log_analysis_start(context)
        
        try:
            # Calculate threshold date
            threshold_date = datetime.now() - timedelta(days=self.days_threshold)
            
            # Ensure timestamp columns are datetime type
            stats_df_copy = stats_df.copy()
            if 'LastAccessTimeStamp' in stats_df_copy.columns:
                stats_df_copy['LastAccessTimeStamp'] = pd.to_datetime(
                    stats_df_copy['LastAccessTimeStamp'], errors='coerce'
                )
            
            # Filter for tables with no recent access
            # Tables with NULL LastAccessTimeStamp or access older than threshold
            unused_mask = (
                stats_df_copy['LastAccessTimeStamp'].isna() |
                (stats_df_copy['LastAccessTimeStamp'] < threshold_date)
            )
            
            unused_stats = stats_df_copy[unused_mask].copy()
            
            # Add analysis-specific columns
            unused_stats['days_since_last_access'] = (
                (datetime.now() - unused_stats['LastAccessTimeStamp']).dt.days
            ).fillna(-1)  # -1 for never accessed
            
            unused_stats['access_status'] = unused_stats['days_since_last_access'].apply(
                lambda x: 'Never Accessed' if x == -1 else f'Not accessed for {int(x)} days'
            )
            
            unused_stats['unused_threshold_days'] = self.days_threshold
            
            # Add rule metadata
            result_df = self.add_metadata_columns(unused_stats)
            
            # Log summary
            if not result_df.empty:
                never_accessed = (result_df['days_since_last_access'] == -1).sum()
                old_access = len(result_df) - never_accessed
                
                logger.info(f"Found {len(result_df)} unused statistics: "
                           f"{never_accessed} never accessed, {old_access} not accessed for >{self.days_threshold} days")
                
                # Log database/table distribution
                db_count = result_df['DatabaseName'].nunique()
                table_count = result_df['TableName'].nunique()
                logger.debug(f"Unused statistics span {db_count} databases, {table_count} tables")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule01Unused analysis: {str(e)}")
            raise
    
    def get_unused_by_database(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get summary of unused statistics by database.
        
        Args:
            result_df: DataFrame returned by analyze method
        
        Returns:
            DataFrame with unused statistics summary by database
        """
        if result_df.empty:
            return pd.DataFrame()
        
        summary = (
            result_df
            .groupby('DatabaseName')
            .agg(
                unused_stats_count=pd.NamedAgg(column='StatsName', aggfunc='size'),
                unique_tables_count=pd.NamedAgg(column='TableName', aggfunc='nunique'),
                never_accessed_count=pd.NamedAgg(
                    column='days_since_last_access', 
                    aggfunc=lambda x: (x == -1).sum()
                ),
                avg_days_since_access=pd.NamedAgg(
                    column='days_since_last_access',
                    aggfunc=lambda x: x[x != -1].mean() if (x != -1).any() else 0
                )
            )
            .reset_index()
        )
        
        summary['rule_id'] = self.rule_id
        summary['analysis_date'] = self.analysis_date
        
        return summary
    
    def get_unused_by_table(self, result_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
        """
        Get top N tables with most unused statistics.
        
        Args:
            result_df: DataFrame returned by analyze method
            top_n: Number of top tables to return
        
        Returns:
            DataFrame with top tables by unused statistics count
        """
        if result_df.empty:
            return pd.DataFrame()
        
        table_summary = (
            result_df
            .groupby(['DatabaseName', 'TableName'])
            .agg(
                unused_stats_count=pd.NamedAgg(column='StatsName', aggfunc='size'),
                days_since_last_access=pd.NamedAgg(
                    column='days_since_last_access',
                    aggfunc=lambda x: x[x != -1].max() if (x != -1).any() else -1
                ),
                access_status=pd.NamedAgg(column='access_status', aggfunc='first')
            )
            .reset_index()
            .sort_values('unused_stats_count', ascending=False)
            .head(top_n)
        )
        
        table_summary['rule_id'] = self.rule_id
        table_summary['analysis_date'] = self.analysis_date
        
        return table_summary
    
    def get_recommendations(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate cleanup recommendations for unused statistics.
        
        Args:
            result_df: DataFrame returned by analyze method
        
        Returns:
            DataFrame with cleanup recommendations
        """
        if result_df.empty:
            return pd.DataFrame()
        
        # Prioritize recommendations based on days since last access
        recommendations = result_df.copy()
        
        # Add priority levels
        def get_priority(days_since_access):
            if days_since_access == -1:
                return 'HIGH'  # Never accessed
            elif days_since_access > 90:
                return 'HIGH'  # Not accessed for > 90 days
            elif days_since_access > self.days_threshold:
                return 'MEDIUM'  # Not accessed for > threshold
            else:
                return 'LOW'  # Recently accessed
        
        recommendations['priority'] = recommendations['days_since_last_access'].apply(get_priority)
        recommendations['recommendation'] = 'DROP STATISTICS'
        recommendations['reason'] = recommendations['access_status']
        
        # Sort by priority (HIGH first)
        priority_order = {'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
        recommendations = recommendations.sort_values('priority', key=lambda x: x.map(priority_order))
        
        return recommendations[['DatabaseName', 'TableName', 'ColumnName', 'StatsName', 
                               'priority', 'recommendation', 'reason', 'days_since_last_access']]


if __name__ == "__main__":
    # Example usage for testing
    try:
        # Create sample data
        sample_stats = pd.DataFrame({
            'DatabaseName': ['DB1', 'DB1', 'DB2', 'DB2', 'DB3'],
            'TableName': ['Table1', 'Table2', 'Table1', 'Table3', 'Table1'],
            'ColumnName': ['Col1', 'Col2', 'Col1', 'Col1', 'Col2'],
            'StatsName': ['Stats1', 'Stats2', 'Stats3', 'Stats4', 'Stats5'],
            'StatsType': ['I', 'I', 'I', 'I', 'I'],
            'LastAccessTimeStamp': [
                datetime.now() - timedelta(days=45),  # Old access
                None,  # Never accessed
                datetime.now() - timedelta(days=10),  # Recent access
                datetime.now() - timedelta(days=100), # Very old access
                None   # Never accessed
            ]
        })
        
        context = {'stats_df': sample_stats}
        
        # Test the rule
        rule = Rule01Unused(days_threshold=30)
        result = rule.analyze(context)
        
        print(f"Found {len(result)} unused statistics:")
        if not result.empty:
            print(result[['DatabaseName', 'TableName', 'ColumnName', 'access_status']])
            
            # Test summary functions
            db_summary = rule.get_unused_by_database(result)
            print(f"\nDatabase summary: {len(db_summary)} databases")
            
            table_summary = rule.get_unused_by_table(result)
            print(f"Table summary: {len(table_summary)} tables")
            
            recommendations = rule.get_recommendations(result)
            print(f"\nRecommendations: {len(recommendations)} items")
        
    except Exception as e:
        print(f"Error testing Rule01Unused: {str(e)}")
