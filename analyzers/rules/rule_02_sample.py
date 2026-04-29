"""
Rule 02: Sample Candidates Detection

This rule identifies statistics that are being collected at 100% (SampleSizePct == 0 or 100)
on large tables where the UniqueValueCount is nearly equal to RowCount (Ratio >= 0.95).
These columns are candidates for USING SAMPLE to optimize collection time and resources.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule

# Configure logging
logger = logging.getLogger(__name__)


class Rule02Sample(BaseStatsRule):
    """
    Rule to detect sample candidates for statistics collection.
    
    This rule analyzes statistics on large tables that are being collected
    at 100% and have high cardinality, making them good candidates for
    sample-based statistics collection.
    """
    
    def __init__(self, table_size_threshold_gb: float = 50.0, cardinality_ratio: float = 0.95):
        """
        Initialize the sample candidates rule.
        
        Args:
            table_size_threshold_gb: Minimum table size in GB to consider (default: 50.0)
            cardinality_ratio: Minimum ratio of UniqueValueCount/RowCount (default: 0.95)
        """
        super().__init__(
            rule_id="rule_02_sample",
            rule_name="Sample Candidates",
            description="Detects statistics on large tables that could benefit from USING SAMPLE"
        )
        
        self.table_size_threshold_gb = table_size_threshold_gb
        self.cardinality_ratio = cardinality_ratio
        logger.info(f"Initialized Rule02Sample with size threshold {table_size_threshold_gb}GB and cardinality ratio {cardinality_ratio}")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze statistics data to find sample candidates.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df'
        
        Returns:
            DataFrame containing sample candidates with metadata columns
        """
        # Validate context
        self.validate_context(context, ['stats_df'])
        
        stats_df = context['stats_df']
        
        self.log_analysis_start(context)
        
        try:
            # Ensure numeric columns are properly typed
            stats_df_copy = stats_df.copy()
            
            # Convert SampleSizePct to numeric, handling nulls
            if 'SampleSizePct' in stats_df_copy.columns:
                stats_df_copy['SampleSizePct'] = pd.to_numeric(
                    stats_df_copy['SampleSizePct'], errors='coerce'
                )
            
            # Convert RowCount and UniqueValueCount to numeric
            if 'RowCount' in stats_df_copy.columns:
                stats_df_copy['RowCount'] = pd.to_numeric(
                    stats_df_copy['RowCount'], errors='coerce'
                )
            
            if 'UniqueValueCount' in stats_df_copy.columns:
                stats_df_copy['UniqueValueCount'] = pd.to_numeric(
                    stats_df_copy['UniqueValueCount'], errors='coerce'
                )
            
            # Convert TableSizeGB to numeric
            if 'TableSizeGB' in stats_df_copy.columns:
                stats_df_copy['TableSizeGB'] = pd.to_numeric(
                    stats_df_copy['TableSizeGB'], errors='coerce'
                )
            
            # Calculate cardinality ratio (UniqueValueCount / RowCount)
            # Handle division by zero and null values
            stats_df_copy['cardinality_ratio'] = (
                stats_df_copy['UniqueValueCount'] / stats_df_copy['RowCount']
            ).fillna(0)
            
            # Filter for sample candidates:
            # 1. SampleSizePct is 0 or 100 (full collection)
            # 2. Cardinality ratio >= threshold (high cardinality)
            # 3. Table size >= threshold (large tables)
            # 4. RowCount > 0 (avoid division by zero issues)
            sample_mask = (
                (
                    (stats_df_copy['SampleSizePct'] == 0) | 
                    (stats_df_copy['SampleSizePct'] == 100) |
                    (stats_df_copy['SampleSizePct'].isna())
                ) &
                (stats_df_copy['cardinality_ratio'] >= self.cardinality_ratio) &
                (stats_df_copy['TableSizeGB'] >= self.table_size_threshold_gb) &
                (stats_df_copy['RowCount'] > 0)
            )
            
            sample_candidates = stats_df_copy[sample_mask].copy()
            
            # Add analysis-specific columns
            sample_candidates['table_size_threshold_gb'] = self.table_size_threshold_gb
            sample_candidates['cardinality_ratio_threshold'] = self.cardinality_ratio
            sample_candidates['recommendation'] = 'USING SAMPLE'
            sample_candidates['reason'] = (
                sample_candidates.apply(
                    lambda row: f"Large table ({row['TableSizeGB']:.2f}GB) with high cardinality ({row['cardinality_ratio']:.2f}) collected at full sample",
                    axis=1
                )
            )
            
            # Add rule metadata
            result_df = self.add_metadata_columns(sample_candidates)
            
            # Log summary
            if not result_df.empty:
                avg_table_size = result_df['TableSizeGB'].mean()
                avg_cardinality = result_df['cardinality_ratio'].mean()
                
                logger.info(f"Found {len(result_df)} sample candidates")
                logger.info(f"Average table size: {avg_table_size:.2f}GB, Average cardinality ratio: {avg_cardinality:.2f}")
                
                # Log database/table distribution
                db_count = result_df['DatabaseName'].nunique()
                table_count = result_df['TableName'].nunique()
                logger.debug(f"Sample candidates span {db_count} databases, {table_count} tables")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule02Sample analysis: {str(e)}")
            raise
    
    def get_sample_candidates_by_database(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get summary of sample candidates by database.
        
        Args:
            result_df: DataFrame returned by analyze method
        
        Returns:
            DataFrame with sample candidates summary by database
        """
        if result_df.empty:
            return pd.DataFrame()
        
        summary = (
            result_df
            .groupby('DatabaseName')
            .agg(
                sample_candidates_count=pd.NamedAgg(column='StatsName', aggfunc='size'),
                unique_tables_count=pd.NamedAgg(column='TableName', aggfunc='nunique'),
                avg_table_size_gb=pd.NamedAgg(column='TableSizeGB', aggfunc='mean'),
                avg_cardinality_ratio=pd.NamedAgg(column='cardinality_ratio', aggfunc='mean'),
                total_table_size_gb=pd.NamedAgg(column='TableSizeGB', aggfunc='sum')
            )
            .reset_index()
        )
        
        summary['rule_id'] = self.rule_id
        summary['analysis_date'] = self.analysis_date
        
        return summary
    
    def get_sample_candidates_by_table(self, result_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
        """
        Get top N tables with most sample candidates.
        
        Args:
            result_df: DataFrame returned by analyze method
            top_n: Number of top tables to return
        
        Returns:
            DataFrame with top tables by sample candidates count
        """
        if result_df.empty:
            return pd.DataFrame()
        
        table_summary = (
            result_df
            .groupby(['DatabaseName', 'TableName'])
            .agg(
                sample_candidates_count=pd.NamedAgg(column='StatsName', aggfunc='size'),
                table_size_gb=pd.NamedAgg(column='TableSizeGB', aggfunc='first'),
                avg_cardinality_ratio=pd.NamedAgg(column='cardinality_ratio', aggfunc='mean')
            )
            .reset_index()
            .sort_values(['table_size_gb', 'sample_candidates_count'], ascending=[False, False])
            .head(top_n)
        )
        
        table_summary['rule_id'] = self.rule_id
        table_summary['analysis_date'] = self.analysis_date
        
        return table_summary
    
    def get_recommendations(self, result_df: pd.DataFrame, sample_percent: float = 5.0) -> pd.DataFrame:
        """
        Generate USING SAMPLE recommendations.
        
        Args:
            result_df: DataFrame returned by analyze method
            sample_percent: Sample percentage to recommend (default: 5.0)
        
        Returns:
            DataFrame with sample recommendations
        """
        if result_df.empty:
            return pd.DataFrame()
        
        recommendations = result_df.copy()
        recommendations['recommended_sample_percent'] = sample_percent
        recommendations['estimated_time_savings'] = 'HIGH'  # Full to sample typically saves significant time
        
        return recommendations[['DatabaseName', 'TableName', 'ColumnName', 'StatsName', 
                               'table_size_gb', 'cardinality_ratio', 'recommended_sample_percent',
                               'estimated_time_savings', 'reason']]


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
            'SampleSizePct': [0, 100, 50, 0, 100],  # 0 or 100 = full collection
            'RowCount': [1000000, 500000, 100000, 2000000, 800000],
            'UniqueValueCount': [980000, 490000, 50000, 1950000, 780000],  # High cardinality
            'TableSizeGB': [60.0, 55.0, 10.0, 80.0, 52.0]  # Large tables
        })
        
        context = {'stats_df': sample_stats}
        
        # Test the rule
        rule = Rule02Sample(table_size_threshold_gb=50.0, cardinality_ratio=0.95)
        result = rule.analyze(context)
        
        print(f"Found {len(result)} sample candidates:")
        if not result.empty:
            print(result[['DatabaseName', 'TableName', 'ColumnName', 'TableSizeGB', 'cardinality_ratio']])
            
            # Test summary functions
            db_summary = rule.get_sample_candidates_by_database(result)
            print(f"\nDatabase summary: {len(db_summary)} databases")
            
            table_summary = rule.get_sample_candidates_by_table(result)
            print(f"Table summary: {len(table_summary)} tables")
            
            recommendations = rule.get_recommendations(result)
            print(f"\nRecommendations: {len(recommendations)} items")
        
    except Exception as e:
        print(f"Error testing Rule02Sample: {str(e)}")
