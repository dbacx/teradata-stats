"""
Rule 06: Stale Statistics Detection

This rule identifies statistics that have not been collected recently,
indicating they may be outdated and not reflecting current data distributions,
which could lead to suboptimal query plans.
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

# Configure logging
logger = logging.getLogger(__name__)


class Rule06Stale(BaseStatsRule):
    """
    Rule to detect stale statistics based on collection timestamps.
    
    This rule analyzes statistics that have not been collected within
    a specified time threshold, indicating they may need to be refreshed
    to maintain query optimization effectiveness.
    """
    
    def __init__(self, days_threshold: int = 15):
        """
        Initialize the stale statistics rule.
        
        Args:
            days_threshold: Number of days since last collection to consider as stale (default: 15)
        """
        super().__init__(
            rule_id="rule_06_stale",
            rule_name="Stale Stats",
            description="Detects statistics that have not been collected recently and may be outdated"
        )
        
        self.days_threshold = days_threshold
        logger.info(f"Initialized Rule06Stale with {days_threshold} days threshold")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find stale statistics.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing stale statistics with metadata columns
        """
        # Validate context
        self.validate_context(context, ['stats_df'])
        
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            # Calculate threshold date
            threshold_date = datetime.now() - timedelta(days=self.days_threshold)
            
            # Ensure timestamp columns are datetime type
            stats_df_copy = stats_df.copy()
            if 'LastCollectTimeStamp' in stats_df_copy.columns:
                stats_df_copy['LastCollectTimeStamp'] = pd.to_datetime(
                    stats_df_copy['LastCollectTimeStamp'], errors='coerce'
                )
            
            # Filter for statistics older than threshold
            # Also include NULL timestamps (never collected)
            stale_mask = (
                stats_df_copy['LastCollectTimeStamp'].isna() |
                (stats_df_copy['LastCollectTimeStamp'] < threshold_date)
            )
            
            stale_stats = stats_df_copy[stale_mask].copy()
            
            # Add analysis-specific columns
            stale_stats['days_since_collection'] = (
                (datetime.now() - stale_stats['LastCollectTimeStamp']).dt.days
            ).fillna(-1)  # -1 for never collected
            
            stale_stats['collection_status'] = stale_stats['days_since_collection'].apply(
                lambda x: 'Never Collected' if x == -1 else f'Not collected for {int(x)} days'
            )
            
            stale_stats['stale_threshold_days'] = self.days_threshold
            
            # Add priority based on how stale the statistics are
            def get_stale_priority(days_since_collection):
                if days_since_collection == -1:
                    return 'CRITICAL'  # Never collected
                elif days_since_collection > 30:
                    return 'HIGH'  # Very stale
                elif days_since_collection > self.days_threshold:
                    return 'MEDIUM'  # Moderately stale
                else:
                    return 'LOW'  # Recently collected
            
            stale_stats['priority'] = stale_stats['days_since_collection'].apply(get_stale_priority)
            
            # Add rule metadata
            result_df = self.add_metadata_columns(stale_stats)
            
            # Log summary
            if not result_df.empty:
                never_collected = (result_df['days_since_collection'] == -1).sum()
                old_collection = len(result_df) - never_collected
                
                # Count by priority
                priority_counts = result_df['priority'].value_counts()
                
                logger.info(f"Found {len(result_df)} stale statistics: "
                           f"{never_collected} never collected, {old_collection} not collected for >{self.days_threshold} days")
                logger.info(f"Priority breakdown: {dict(priority_counts)}")
                
                # Log database/table distribution
                db_count = result_df['DatabaseName'].nunique()
                table_count = result_df['TableName'].nunique()
                logger.debug(f"Stale statistics span {db_count} databases, {table_count} tables")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule06Stale analysis: {str(e)}")
            raise
    
    def get_stale_by_database(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get summary of stale statistics by database.
        
        Args:
            result_df: DataFrame returned by analyze method
        
        Returns:
            DataFrame with stale statistics summary by database
        """
        if result_df.empty:
            return pd.DataFrame()
        
        summary = (
            result_df
            .groupby('DatabaseName')
            .agg(
                stale_stats_count=pd.NamedAgg(column='StatsName', aggfunc='size'),
                unique_tables_count=pd.NamedAgg(column='TableName', aggfunc='nunique'),
                never_collected_count=pd.NamedAgg(
                    column='days_since_collection', 
                    aggfunc=lambda x: (x == -1).sum()
                ),
                avg_days_since_collection=pd.NamedAgg(
                    column='days_since_collection',
                    aggfunc=lambda x: x[x != -1].mean() if (x != -1).any() else 0
                ),
                critical_count=pd.NamedAgg(
                    column='priority',
                    aggfunc=lambda x: (x == 'CRITICAL').sum()
                ),
                high_count=pd.NamedAgg(
                    column='priority',
                    aggfunc=lambda x: (x == 'HIGH').sum()
                )
            )
            .reset_index()
        )
        
        summary['rule_id'] = self.rule_id
        summary['analysis_date'] = self.analysis_date
        
        return summary
    
    def get_stale_by_table(self, result_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
        """
        Get top N tables with most stale statistics.
        
        Args:
            result_df: DataFrame returned by analyze method
            top_n: Number of top tables to return
        
        Returns:
            DataFrame with top tables by stale statistics count
        """
        if result_df.empty:
            return pd.DataFrame()
        
        table_summary = (
            result_df
            .groupby(['DatabaseName', 'TableName'])
            .agg(
                stale_stats_count=pd.NamedAgg(column='StatsName', aggfunc='size'),
                days_since_collection=pd.NamedAgg(
                    column='days_since_collection',
                    aggfunc=lambda x: x[x != -1].max() if (x != -1).any() else -1
                ),
                collection_status=pd.NamedAgg(column='collection_status', aggfunc='first'),
                critical_count=pd.NamedAgg(
                    column='priority',
                    aggfunc=lambda x: (x == 'CRITICAL').sum()
                )
            )
            .reset_index()
            .sort_values('stale_stats_count', ascending=False)
            .head(top_n)
        )
        
        table_summary['rule_id'] = self.rule_id
        table_summary['analysis_date'] = self.analysis_date
        
        return table_summary
    
    def get_recommendations(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate refresh recommendations for stale statistics.
        
        Args:
            result_df: DataFrame returned by analyze method
        
        Returns:
            DataFrame with refresh recommendations
        """
        if result_df.empty:
            return pd.DataFrame()
        
        # Prioritize recommendations based on priority and days since collection
        recommendations = result_df.copy()
        
        recommendations['recommendation'] = 'COLLECT STATISTICS'
        recommendations['reason'] = recommendations['collection_status']
        
        # Sort by priority (CRITICAL first)
        priority_order = {'CRITICAL': 1, 'HIGH': 2, 'MEDIUM': 3, 'LOW': 4}
        recommendations = recommendations.sort_values(['priority', 'days_since_collection'], 
                                                     key=lambda x: x.map(priority_order) if x.name == 'priority' else x)
        
        return recommendations[['DatabaseName', 'TableName', 'ColumnName', 'StatsName', 
                               'priority', 'recommendation', 'reason', 'days_since_collection']]
    
    def get_collection_schedule(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate a recommended collection schedule for stale statistics.
        
        Args:
            result_df: DataFrame returned by analyze method
        
        Returns:
            DataFrame with recommended collection schedule
        """
        if result_df.empty:
            return pd.DataFrame()
        
        schedule = result_df.copy()
        
        # Assign collection urgency based on priority
        def get_collection_urgency(priority, days_since_collection):
            if priority == 'CRITICAL':
                return 'IMMEDIATE'
            elif priority == 'HIGH':
                return 'WITHIN_24_HOURS'
            elif priority == 'MEDIUM':
                return 'WITHIN_3_DAYS'
            else:
                return 'WITHIN_7_DAYS'
        
        schedule['collection_urgency'] = schedule.apply(
            lambda row: get_collection_urgency(row['priority'], row['days_since_collection']), 
            axis=1
        )
        
        # Add estimated collection time (rough estimate based on table size)
        if 'TableSizeGB' in schedule.columns:
            def estimate_collection_time(size_gb):
                if size_gb < 1:
                    return 'MINUTES'
                elif size_gb < 10:
                    return 'HOURS'
                else:
                    return 'EXTENDED'
            
            schedule['estimated_collection_time'] = schedule['TableSizeGB'].apply(estimate_collection_time)
        else:
            schedule['estimated_collection_time'] = 'UNKNOWN'
        
        return schedule[['DatabaseName', 'TableName', 'ColumnName', 'StatsName', 
                        'priority', 'collection_urgency', 'estimated_collection_time', 
                        'days_since_collection']]


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
            'LastCollectTimeStamp': [
                datetime.now() - timedelta(days=25),  # Stale
                None,  # Never collected
                datetime.now() - timedelta(days=5),   # Recent
                datetime.now() - timedelta(days=45),  # Very stale
                None   # Never collected
            ],
            'TableSizeGB': [0.5, 2.0, 1.5, 15.0, 0.8]
        })
        
        context = {'stats_df': sample_stats}
        
        # Test the rule
        rule = Rule06Stale(days_threshold=15)
        result = rule.analyze(context)
        
        print(f"Found {len(result)} stale statistics:")
        if not result.empty:
            print(result[['DatabaseName', 'TableName', 'ColumnName', 'priority', 'collection_status']])
            
            # Test summary functions
            db_summary = rule.get_stale_by_database(result)
            print(f"\nDatabase summary: {len(db_summary)} databases")
            
            table_summary = rule.get_stale_by_table(result)
            print(f"Table summary: {len(table_summary)} tables")
            
            recommendations = rule.get_recommendations(result)
            print(f"\nRecommendations: {len(recommendations)} items")
            
            schedule = rule.get_collection_schedule(result)
            print(f"\nCollection schedule: {len(schedule)} items")
        
    except Exception as e:
        print(f"Error testing Rule06Stale: {str(e)}")
