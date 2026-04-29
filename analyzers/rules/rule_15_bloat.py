"""
Rule 15: Dictionary Bloat Detection

This rule identifies tables with an excessive number of statistics,
indicating potential dictionary bloat that can impact performance and maintenance.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

# Configure logging
logger = logging.getLogger(__name__)


class Rule15Bloat(BaseStatsRule):
    """
    Rule to detect dictionary bloat in tables.
    
    This rule analyzes tables that have an excessive number of statistics,
    which can lead to dictionary bloat and impact system performance.
    """
    
    def __init__(self, max_stats_threshold: int = 50):
        """
        Initialize the dictionary bloat rule.
        
        Args:
            max_stats_threshold: Maximum number of statistics per table before considering as bloat (default: 50)
        """
        super().__init__(
            rule_id="rule_15_bloat",
            rule_name="Dictionary Bloat",
            description="Detects tables with excessive statistics indicating dictionary bloat"
        )
        
        self.max_stats_threshold = max_stats_threshold
        logger.info(f"Initialized Rule15Bloat with max stats threshold {max_stats_threshold}")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find tables with dictionary bloat.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing bloated tables with metadata columns
        """
        # Validate context
        self.validate_context(context, ['stats_df'])
        
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            # Group by DatabaseName and TableName to count statistics per table
            # Count non-null ColumnName entries
            stats_count = (
                stats_df
                .groupby(['DatabaseName', 'TableName'])
                .agg(
                    stats_count=pd.NamedAgg(column='ColumnName', aggfunc='count'),
                    table_size_gb=pd.NamedAgg(column='TableSizeGB', aggfunc='first'),
                    total_row_count=pd.NamedAgg(column='RowCount', aggfunc='first')
                )
                .reset_index()
            )
            
            # Filter for tables exceeding the threshold
            bloat_mask = stats_count['stats_count'] > self.max_stats_threshold
            bloated_tables = stats_count[bloat_mask].copy()
            
            if bloated_tables.empty:
                self.log_analysis_end(bloated_tables)
                return bloated_tables
            
            # Add analysis-specific columns
            bloated_tables['excess_stats_count'] = bloated_tables['stats_count'] - self.max_stats_threshold
            bloated_tables['bloat_percentage'] = (
                (bloated_tables['stats_count'] / self.max_stats_threshold - 1) * 100
            )
            bloated_tables['max_stats_threshold'] = self.max_stats_threshold
            bloated_tables['recommendation'] = 'DROP STATISTICS'
            bloated_tables['reason'] = (
                bloated_tables.apply(
                    lambda row: f"Table has {row['stats_count']} statistics ({row['excess_stats_count']} excess over threshold {self.max_stats_threshold})",
                    axis=1
                )
            )
            
            # Add priority based on severity
            def get_bloat_priority(excess_count):
                if excess_count > 100:
                    return 'CRITICAL'
                elif excess_count > 50:
                    return 'HIGH'
                elif excess_count > 20:
                    return 'MEDIUM'
                else:
                    return 'LOW'
            
            bloated_tables['priority'] = bloated_tables['excess_stats_count'].apply(get_bloat_priority)
            
            # Add rule metadata
            result_df = self.add_metadata_columns(bloated_tables)
            
            # Log summary
            if not result_df.empty:
                avg_stats = result_df['stats_count'].mean()
                max_stats = result_df['stats_count'].max()
                avg_excess = result_df['excess_stats_count'].mean()
                
                logger.info(f"Found {len(result_df)} tables with dictionary bloat")
                logger.info(f"Average stats per bloated table: {avg_stats:.1f}, Maximum: {max_stats}")
                logger.info(f"Average excess stats: {avg_excess:.1f}")
                
                # Log database distribution
                db_count = result_df['DatabaseName'].nunique()
                logger.debug(f"Bloated tables span {db_count} databases")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule15Bloat analysis: {str(e)}")
            raise
    
    def get_bloat_by_database(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get summary of dictionary bloat by database.
        
        Args:
            result_df: DataFrame returned by analyze method
        
        Returns:
            DataFrame with bloat summary by database
        """
        if result_df.empty:
            return pd.DataFrame()
        
        summary = (
            result_df
            .groupby('DatabaseName')
            .agg(
                bloated_tables_count=pd.NamedAgg(column='TableName', aggfunc='nunique'),
                total_stats_count=pd.NamedAgg(column='stats_count', aggfunc='sum'),
                avg_stats_per_table=pd.NamedAgg(column='stats_count', aggfunc='mean'),
                total_excess_stats=pd.NamedAgg(column='excess_stats_count', aggfunc='sum'),
                max_stats_in_db=pd.NamedAgg(column='stats_count', aggfunc='max'),
                total_table_size_gb=pd.NamedAgg(column='table_size_gb', aggfunc='sum')
            )
            .reset_index()
        )
        
        summary['rule_id'] = self.rule_id
        summary['analysis_date'] = self.analysis_date
        
        return summary
    
    def get_most_bloated_tables(self, result_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
        """
        Get top N most bloated tables.
        
        Args:
            result_df: DataFrame returned by analyze method
            top_n: Number of top tables to return
        
        Returns:
            DataFrame with most bloated tables
        """
        if result_df.empty:
            return pd.DataFrame()
        
        most_bloated = (
            result_df
            .sort_values(['stats_count', 'excess_stats_count'], ascending=[False, False])
            .head(top_n)
            .copy()
        )
        
        most_bloated['rule_id'] = self.rule_id
        most_bloated['analysis_date'] = self.analysis_date
        
        return most_bloated
    
    def get_recommendations(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate cleanup recommendations for bloated tables.
        
        Args:
            result_df: DataFrame returned by analyze method
        
        Returns:
            DataFrame with cleanup recommendations
        """
        if result_df.empty:
            return pd.DataFrame()
        
        recommendations = result_df.copy()
        
        # Sort by priority (CRITICAL first)
        priority_order = {'CRITICAL': 1, 'HIGH': 2, 'MEDIUM': 3, 'LOW': 4}
        recommendations = recommendations.sort_values('priority', key=lambda x: x.map(priority_order))
        
        return recommendations[['DatabaseName', 'TableName', 'stats_count', 'excess_stats_count',
                               'priority', 'recommendation', 'reason', 'bloat_percentage']]
    
    def get_cleanup_candidates(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get detailed list of statistics that could be dropped from bloated tables.
        
        Args:
            result_df: DataFrame returned by analyze method
        
        Returns:
            DataFrame with potential statistics to drop (requires original stats_df context)
        """
        if result_df.empty:
            return pd.DataFrame()
        
        # This would require access to the original stats_df to identify specific stats
        # For now, return a summary of tables that need cleanup
        cleanup_summary = result_df.copy()
        cleanup_summary['cleanup_action'] = 'REVIEW_AND_DROP_REDUNDANT_STATS'
        cleanup_summary['estimated_space_savings'] = 'HIGH'  # Dictionary bloat typically saves significant space
        
        return cleanup_summary[['DatabaseName', 'TableName', 'stats_count', 'excess_stats_count',
                                 'priority', 'cleanup_action', 'estimated_space_savings']]


if __name__ == "__main__":
    # Example usage for testing
    try:
        # Create sample data
        sample_stats = pd.DataFrame({
            'DatabaseName': ['DB1', 'DB1', 'DB1', 'DB2', 'DB2', 'DB3'],
            'TableName': ['Table1', 'Table1', 'Table2', 'Table1', 'Table1', 'Table1'],
            'ColumnName': ['Col1', 'Col2', 'Col1', 'Col1', 'Col2', 'Col1'],
            'StatsName': ['Stats1', 'Stats2', 'Stats3', 'Stats4', 'Stats5', 'Stats6'],
            'StatsType': ['I', 'I', 'I', 'I', 'I', 'I'],
            'RowCount': [1000000, 1000000, 500000, 2000000, 2000000, 800000],
            'TableSizeGB': [60.0, 60.0, 30.0, 80.0, 80.0, 52.0]
        })
        
        # Add more stats to Table1 in DB1 to make it bloated
        for i in range(7, 55):
            sample_stats = pd.concat([
                sample_stats,
                pd.DataFrame({
                    'DatabaseName': ['DB1'],
                    'TableName': ['Table1'],
                    'ColumnName': [f'Col{i}'],
                    'StatsName': [f'Stats{i}'],
                    'StatsType': ['I'],
                    'RowCount': [1000000],
                    'TableSizeGB': [60.0]
                })
            ], ignore_index=True)
        
        context = {'stats_df': sample_stats}
        
        # Test the rule
        rule = Rule15Bloat(max_stats_threshold=50)
        result = rule.analyze(context)
        
        print(f"Found {len(result)} bloated tables:")
        if not result.empty:
            print(result[['DatabaseName', 'TableName', 'stats_count', 'excess_stats_count', 'priority']])
            
            # Test summary functions
            db_summary = rule.get_bloat_by_database(result)
            print(f"\nDatabase summary: {len(db_summary)} databases")
            
            most_bloated = rule.get_most_bloated_tables(result)
            print(f"Most bloated tables: {len(most_bloated)} tables")
            
            recommendations = rule.get_recommendations(result)
            print(f"\nRecommendations: {len(recommendations)} items")
        
    except Exception as e:
        print(f"Error testing Rule15Bloat: {str(e)}")
