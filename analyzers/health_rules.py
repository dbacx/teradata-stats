"""
Health Rules Analyzer for Teradata Statistics

This module provides analysis functions to detect statistics health issues
using vectorized pandas operations for optimal performance.
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple

# Configure logging
logger = logging.getLogger(__name__)


class StatsAnalyzer:
    """
    Analyzer class for detecting statistics health issues in Teradata databases.
    
    Uses vectorized pandas operations for efficient analysis of large datasets
    without touching the database after initial data extraction.
    """
    
    def __init__(self):
        """Initialize the StatsAnalyzer with default configuration."""
        self.analysis_date = datetime.now()
        logger.info(f"StatsAnalyzer initialized with analysis date: {self.analysis_date}")
    
    def detect_stale_stats(self, df_stats: pd.DataFrame, days_threshold: int = 15) -> pd.DataFrame:
        """
        Detect stale statistics based on collection timestamp threshold.
        
        Uses vectorized pandas operations to filter statistics older than
        the specified threshold without loops.
        
        Args:
            df_stats: DataFrame containing statistics metadata with LastCollectTimeStamp column
            days_threshold: Number of days to consider statistics as stale (default: 15)
            
        Returns:
            DataFrame containing only stale statistics records
            
        Raises:
            ValueError: If required columns are missing from input DataFrame
        """
        if df_stats.empty:
            logger.warning("Empty DataFrame provided to detect_stale_stats")
            return pd.DataFrame()
        
        required_columns = ['LastCollectTimeStamp', 'DatabaseName', 'TableName']
        missing_columns = [col for col in required_columns if col not in df_stats.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        try:
            # Calculate threshold date using vectorized operations
            threshold_date = self.analysis_date - timedelta(days=days_threshold)
            
            # Ensure LastCollectTimeStamp is datetime type for vectorized comparison
            df_stats_copy = df_stats.copy()
            df_stats_copy['LastCollectTimeStamp'] = pd.to_datetime(df_stats_copy['LastCollectTimeStamp'], errors='coerce')
            
            # Vectorized filtering: find stats older than threshold
            stale_mask = df_stats_copy['LastCollectTimeStamp'] < threshold_date
            
            # Apply mask and create result DataFrame
            stale_stats = df_stats_copy[stale_mask].copy()
            
            # Add analysis metadata
            stale_stats['DaysSinceCollection'] = (self.analysis_date - stale_stats['LastCollectTimeStamp']).dt.days
            stale_stats['AnalysisDate'] = self.analysis_date
            stale_stats['StaleThresholdDays'] = days_threshold
            
            stale_count = len(stale_stats)
            logger.info(f"Detected {stale_count} stale statistics (older than {days_threshold} days)")
            
            if stale_count > 0:
                # Summary statistics for logging
                avg_days_old = stale_stats['DaysSinceCollection'].mean()
                max_days_old = stale_stats['DaysSinceCollection'].max()
                logger.info(f"Stale stats summary - Average age: {avg_days_old:.1f} days, Oldest: {max_days_old} days")
            
            return stale_stats
            
        except Exception as e:
            logger.error(f"Error detecting stale statistics: {str(e)}")
            raise
    
    def detect_dictionary_bloat(self, df_stats: pd.DataFrame, max_stats_per_table: int = 50) -> pd.DataFrame:
        """
        Detect dictionary bloat by identifying tables with excessive statistics.
        
        Uses vectorized pandas groupby operations to count statistics per table
        and identify potential bloat issues.
        
        Args:
            df_stats: DataFrame containing statistics metadata
            max_stats_per_table: Maximum number of statistics considered normal per table (default: 50)
            
        Returns:
            DataFrame containing tables with excessive statistics and their counts
            
        Raises:
            ValueError: If required columns are missing from input DataFrame
        """
        if df_stats.empty:
            logger.warning("Empty DataFrame provided to detect_dictionary_bloat")
            return pd.DataFrame()
        
        required_columns = ['DatabaseName', 'TableName']
        missing_columns = [col for col in required_columns if col not in df_stats.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        try:
            # Vectorized groupby operation to count statistics per table
            stats_count = (
                df_stats
                .groupby(['DatabaseName', 'TableName'])
                .size()
                .reset_index(name='StatsCount')
            )
            
            # Filter tables exceeding the threshold using vectorized comparison
            bloat_mask = stats_count['StatsCount'] > max_stats_per_table
            bloated_tables = stats_count[bloat_mask].copy()
            
            # Add analysis metadata
            bloated_tables['AnalysisDate'] = self.analysis_date
            bloated_tables['MaxStatsThreshold'] = max_stats_per_table
            bloated_tables['ExcessCount'] = bloated_tables['StatsCount'] - max_stats_per_table
            
            # Sort by excess count for prioritization
            bloated_tables = bloated_tables.sort_values('ExcessCount', ascending=False)
            
            bloat_count = len(bloated_tables)
            logger.info(f"Detected {bloat_count} tables with dictionary bloat (>{max_stats_per_table} stats per table)")
            
            if bloat_count > 0:
                # Summary statistics for logging
                total_excess = bloated_tables['ExcessCount'].sum()
                max_excess = bloated_tables['ExcessCount'].max()
                logger.info(f"Dictionary bloat summary - Total excess stats: {total_excess}, Worst case: {max_excess} excess")
                
                # Log top 5 bloated tables
                top_bloated = bloated_tables.head(5)
                logger.info("Top 5 bloated tables:")
                for _, row in top_bloated.iterrows():
                    logger.info(f"  {row['DatabaseName']}.{row['TableName']}: {row['StatsCount']} stats ({row['ExcessCount']} excess)")
            
            return bloated_tables
            
        except Exception as e:
            logger.error(f"Error detecting dictionary bloat: {str(e)}")
            raise
    
    def analyze_table_distribution(self, df_stats: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze the distribution of statistics across databases and tables.
        
        Provides summary statistics for understanding the overall statistics
        landscape using vectorized operations.
        
        Args:
            df_stats: DataFrame containing statistics metadata
            
        Returns:
            DataFrame with distribution analysis results
        """
        if df_stats.empty:
            logger.warning("Empty DataFrame provided to analyze_table_distribution")
            return pd.DataFrame()
        
        try:
            # Database-level analysis using vectorized operations
            db_analysis = (
                df_stats
                .groupby('DatabaseName')
                .agg(
                    TableCount=pd.NamedAgg(column='TableName', aggfunc='nunique'),
                    TotalStats=pd.NamedAgg(column='TableName', aggfunc='size'),
                    AvgStatsPerTable=pd.NamedAgg(column='TableName', aggfunc=lambda x: x.groupby(df_stats.loc[x.index, 'DatabaseName']).transform('size').mean())
                )
                .reset_index()
            )
            
            # Add analysis metadata
            db_analysis['AnalysisDate'] = self.analysis_date
            
            logger.info(f"Analyzed statistics distribution across {len(db_analysis)} databases")
            
            return db_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing table distribution: {str(e)}")
            raise
    
    def generate_health_report(self, df_stats: pd.DataFrame, days_threshold: int = 15, 
                             max_stats_per_table: int = 50) -> dict:
        """
        Generate comprehensive health report combining multiple analyses.
        
        Args:
            df_stats: DataFrame containing statistics metadata
            days_threshold: Threshold for stale statistics detection
            max_stats_per_table: Threshold for dictionary bloat detection
            
        Returns:
            Dictionary containing all analysis results
        """
        logger.info("Generating comprehensive health report")
        
        try:
            # Run all analyses
            stale_stats = self.detect_stale_stats(df_stats, days_threshold)
            dictionary_bloat = self.detect_dictionary_bloat(df_stats, max_stats_per_table)
            distribution = self.analyze_table_distribution(df_stats)
            
            # Compile report
            report = {
                'analysis_date': self.analysis_date,
                'total_statistics': len(df_stats),
                'stale_statistics': stale_stats,
                'dictionary_bloat': dictionary_bloat,
                'distribution_analysis': distribution,
                'summary': {
                    'stale_count': len(stale_stats),
                    'bloat_count': len(dictionary_bloat),
                    'database_count': df_stats['DatabaseName'].nunique() if not df_stats.empty else 0,
                    'table_count': df_stats['TableName'].nunique() if not df_stats.empty else 0
                }
            }
            
            logger.info(f"Health report generated - Stale: {report['summary']['stale_count']}, "
                       f"Bloat: {report['summary']['bloat_count']}, "
                       f"Databases: {report['summary']['database_count']}")
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating health report: {str(e)}")
            raise


if __name__ == "__main__":
    # Example usage
    try:
        # Create sample data for testing
        sample_data = {
            'DatabaseName': ['DB1', 'DB1', 'DB1', 'DB2', 'DB2'] * 10,
            'TableName': ['Table1', 'Table1', 'Table2', 'Table1', 'Table2'] * 10,
            'ColumnName': ['Col1', 'Col2', 'Col1', 'Col1', 'Col1'] * 10,
            'LastCollectTimeStamp': [
                datetime.now() - timedelta(days=20),
                datetime.now() - timedelta(days=10),
                datetime.now() - timedelta(days=30),
                datetime.now() - timedelta(days=5),
                datetime.now() - timedelta(days=1)
            ] * 10
        }
        
        df_sample = pd.DataFrame(sample_data)
        
        # Initialize analyzer
        analyzer = StatsAnalyzer()
        
        # Test stale statistics detection
        stale_results = analyzer.detect_stale_stats(df_sample, days_threshold=15)
        print(f"Stale statistics found: {len(stale_results)}")
        
        # Test dictionary bloat detection
        bloat_results = analyzer.detect_dictionary_bloat(df_sample, max_stats_per_table=15)
        print(f"Tables with bloat found: {len(bloat_results)}")
        
        # Generate comprehensive report
        report = analyzer.generate_health_report(df_sample)
        print(f"Health report generated with {report['summary']['stale_count']} stale stats")
        
    except Exception as e:
        print(f"Error in example usage: {str(e)}")
