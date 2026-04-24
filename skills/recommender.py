"""
DDL Recommender for Teradata Statistics

This module provides automated generation of DDL statements for managing
Teradata statistics based on analysis results from the health rules analyzer.
"""

import logging
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)


class DDLRecommender:
    """
    Generates DDL statements for Teradata statistics management.
    
    This class processes analysis results from health rules and creates
    appropriate DDL statements for collecting or dropping statistics
    to optimize database performance and storage.
    """
    
    def __init__(self):
        """Initialize the DDLRecommender with default configuration."""
        self.generation_date = datetime.now()
        logger.info(f"DDLRecommender initialized with generation date: {self.generation_date}")
    
    def generate_collect_stats(self, df_stale: pd.DataFrame) -> List[str]:
        """
        Generate COLLECT STATISTICS DDL statements for stale statistics.
        
        Iterates over the DataFrame of stale statistics and generates appropriate
        DDL statements to refresh the statistics data.
        
        Args:
            df_stale: DataFrame containing stale statistics with columns:
                     DatabaseName, TableName, ColumnName, StatisticsType, etc.
            
        Returns:
            List of COLLECT STATISTICS DDL statements
            
        Raises:
            ValueError: If required columns are missing from input DataFrame
        """
        if df_stale.empty:
            logger.warning("Empty DataFrame provided to generate_collect_stats")
            return []
        
        required_columns = ['DatabaseName', 'TableName', 'ColumnName']
        missing_columns = [col for col in required_columns if col not in df_stale.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns for collect stats: {missing_columns}")
        
        ddl_statements = []
        
        try:
            # Group by DatabaseName and TableName to optimize DDL generation
            grouped = df_stale.groupby(['DatabaseName', 'TableName'])
            
            for (database_name, table_name), group in grouped:
                # Check if this is table-level statistics (ColumnName = '*')
                table_level_stats = group[group['ColumnName'] == '*']
                
                if not table_level_stats.empty:
                    # Generate table-level COLLECT STATISTICS
                    ddl = f"COLLECT STATISTICS ON {database_name}.{table_name};"
                    ddl_statements.append(ddl)
                    logger.debug(f"Generated table-level collect stats: {database_name}.{table_name}")
                else:
                    # Generate column-level COLLECT STATISTICS
                    # Group columns by StatisticsType to optimize multiple columns of same type
                    column_groups = group.groupby('StatisticsType')
                    
                    for stats_type, column_group in column_groups:
                        column_names = column_group['ColumnName'].unique().tolist()
                        
                        if column_names:
                            # Format column list - handle single vs multiple columns
                            if len(column_names) == 1:
                                column_list = f"({column_names[0]})"
                            else:
                                # Join multiple columns with comma
                                column_list = f"({', '.join(column_names)})"
                            
                            ddl = f"COLLECT STATISTICS ON {database_name}.{table_name} COLUMN {column_list};"
                            ddl_statements.append(ddl)
                            logger.debug(f"Generated column-level collect stats: {database_name}.{table_name} - {column_list}")
            
            logger.info(f"Generated {len(ddl_statements)} COLLECT STATISTICS statements")
            return ddl_statements
            
        except Exception as e:
            logger.error(f"Error generating COLLECT STATISTICS statements: {str(e)}")
            raise
    
    def generate_drop_stats(self, df_bloat: pd.DataFrame) -> List[str]:
        """
        Generate DROP STATISTICS DDL statements for bloated statistics.
        
        Processes DataFrame of tables with excessive statistics and generates
        DDL statements to remove redundant or unnecessary statistics.
        
        Args:
            df_bloat: DataFrame containing bloated statistics with columns:
                     DatabaseName, TableName, StatsCount, ExcessCount, etc.
            
        Returns:
            List of DROP STATISTICS DDL statements
            
        Raises:
            ValueError: If required columns are missing from input DataFrame
        """
        if df_bloat.empty:
            logger.warning("Empty DataFrame provided to generate_drop_stats")
            return []
        
        required_columns = ['DatabaseName', 'TableName']
        missing_columns = [col for col in required_columns if col not in df_bloat.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns for drop stats: {missing_columns}")
        
        ddl_statements = []
        
        try:
            # For each bloated table, generate DROP STATISTICS statements
            for _, row in df_bloat.iterrows():
                database_name = row['DatabaseName']
                table_name = row['TableName']
                excess_count = row.get('ExcessCount', 0)
                
                # Generate DROP STATISTICS statement
                # Note: This is a generic DROP statement - specific column identification
                # would require additional analysis of which specific stats to drop
                ddl = f"DROP STATISTICS ON {database_name}.{table_name};"
                ddl_statements.append(ddl)
                
                logger.debug(f"Generated drop stats: {database_name}.{table_name} (excess: {excess_count})")
            
            logger.info(f"Generated {len(ddl_statements)} DROP STATISTICS statements")
            return ddl_statements
            
        except Exception as e:
            logger.error(f"Error generating DROP STATISTICS statements: {str(e)}")
            raise
    
    def generate_sample_stats(self, df_sample_candidates: pd.DataFrame, sample_percent: float = 5.0) -> List[str]:
        """
        Generate COLLECT STATISTICS USING SAMPLE DDL statements for large tables.
        
        Creates sample-based statistics collection for tables that meet
        criteria for sampling (large size, high cardinality, etc.).
        
        Args:
            df_sample_candidates: DataFrame containing tables suitable for sampling
            sample_percent: Sample percentage for statistics collection (default: 5.0)
            
        Returns:
            List of COLLECT STATISTICS USING SAMPLE DDL statements
        """
        if df_sample_candidates.empty:
            logger.warning("Empty DataFrame provided to generate_sample_stats")
            return []
        
        required_columns = ['DatabaseName', 'TableName']
        missing_columns = [col for col in required_columns if col not in df_sample_candidates.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns for sample stats: {missing_columns}")
        
        ddl_statements = []
        
        try:
            for _, row in df_sample_candidates.iterrows():
                database_name = row['DatabaseName']
                table_name = row['TableName']
                
                # Generate sample-based statistics collection
                ddl = f"COLLECT STATISTICS ON {database_name}.{table_name} USING SAMPLE {sample_percent} PERCENT;"
                ddl_statements.append(ddl)
                
                logger.debug(f"Generated sample stats: {database_name}.{table_name} ({sample_percent}% sample)")
            
            logger.info(f"Generated {len(ddl_statements)} SAMPLE STATISTICS statements")
            return ddl_statements
            
        except Exception as e:
            logger.error(f"Error generating SAMPLE STATISTICS statements: {str(e)}")
            raise
    
    def generate_comprehensive_recommendations(self, health_report: Dict) -> Dict[str, List[str]]:
        """
        Generate comprehensive DDL recommendations from health analysis report.
        
        Processes all analysis results and generates appropriate DDL statements
        for different types of statistics optimization.
        
        Args:
            health_report: Dictionary containing health analysis results
            
        Returns:
            Dictionary with categorized DDL recommendations
        """
        logger.info("Generating comprehensive DDL recommendations")
        
        recommendations = {
            'collect_stats': [],
            'drop_stats': [],
            'sample_stats': [],
            'summary': {}
        }
        
        try:
            # Generate COLLECT STATISTICS for stale data
            if 'stale_statistics' in health_report and not health_report['stale_statistics'].empty:
                recommendations['collect_stats'] = self.generate_collect_stats(health_report['stale_statistics'])
            
            # Generate DROP STATISTICS for bloated data
            if 'dictionary_bloat' in health_report and not health_report['dictionary_bloat'].empty:
                recommendations['drop_stats'] = self.generate_drop_stats(health_report['dictionary_bloat'])
            
            # Generate SAMPLE STATISTICS (placeholder for future implementation)
            # This would require additional analysis to identify sample candidates
            
            # Create summary
            recommendations['summary'] = {
                'total_collect_statements': len(recommendations['collect_stats']),
                'total_drop_statements': len(recommendations['drop_stats']),
                'total_sample_statements': len(recommendations['sample_stats']),
                'generation_date': self.generation_date
            }
            
            total_statements = (recommendations['summary']['total_collect_statements'] + 
                              recommendations['summary']['total_drop_statements'] + 
                              recommendations['summary']['total_sample_statements'])
            
            logger.info(f"Generated {total_statements} total DDL recommendations - "
                       f"Collect: {recommendations['summary']['total_collect_statements']}, "
                       f"Drop: {recommendations['summary']['total_drop_statements']}, "
                       f"Sample: {recommendations['summary']['total_sample_statements']}")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating comprehensive recommendations: {str(e)}")
            raise
    
    def format_ddl_output(self, recommendations: Dict[str, List[str]], 
                         include_headers: bool = True) -> str:
        """
        Format DDL recommendations into a readable output format.
        
        Args:
            recommendations: Dictionary containing DDL statements by category
            include_headers: Whether to include section headers in output
            
        Returns:
            Formatted string containing all DDL statements
        """
        output_lines = []
        
        if include_headers:
            output_lines.append("-- DDL Recommendations for Teradata Statistics Optimization")
            output_lines.append(f"-- Generated on: {self.generation_date}")
            output_lines.append("--")
        
        # Add COLLECT STATISTICS section
        if recommendations.get('collect_stats'):
            if include_headers:
                output_lines.append("-- COLLECT STATISTICS (Stale Data Refresh)")
            output_lines.extend(recommendations['collect_stats'])
            if include_headers:
                output_lines.append("")
        
        # Add DROP STATISTICS section
        if recommendations.get('drop_stats'):
            if include_headers:
                output_lines.append("-- DROP STATISTICS (Dictionary Bloat Cleanup)")
            output_lines.extend(recommendations['drop_stats'])
            if include_headers:
                output_lines.append("")
        
        # Add SAMPLE STATISTICS section
        if recommendations.get('sample_stats'):
            if include_headers:
                output_lines.append("-- SAMPLE STATISTICS (Large Table Optimization)")
            output_lines.extend(recommendations['sample_stats'])
            if include_headers:
                output_lines.append("")
        
        return "\n".join(output_lines)


if __name__ == "__main__":
    # Example usage
    try:
        # Create sample data for testing
        stale_data = {
            'DatabaseName': ['DB1', 'DB1', 'DB2'],
            'TableName': ['Table1', 'Table2', 'Table1'],
            'ColumnName': ['Col1', '*', 'Col2'],
            'StatisticsType': ['COLUMN', 'TABLE', 'COLUMN']
        }
        
        bloat_data = {
            'DatabaseName': ['DB1', 'DB2'],
            'TableName': ['Table1', 'Table2'],
            'StatsCount': [75, 60],
            'ExcessCount': [25, 10]
        }
        
        df_stale = pd.DataFrame(stale_data)
        df_bloat = pd.DataFrame(bloat_data)
        
        # Initialize recommender
        recommender = DDLRecommender()
        
        # Test COLLECT STATISTICS generation
        collect_ddls = recommender.generate_collect_stats(df_stale)
        print(f"Generated {len(collect_ddls)} COLLECT statements:")
        for ddl in collect_ddls:
            print(f"  {ddl}")
        
        # Test DROP STATISTICS generation
        drop_ddls = recommender.generate_drop_stats(df_bloat)
        print(f"\nGenerated {len(drop_ddls)} DROP statements:")
        for ddl in drop_ddls:
            print(f"  {ddl}")
        
        # Test comprehensive recommendations
        health_report = {
            'stale_statistics': df_stale,
            'dictionary_bloat': df_bloat
        }
        
        recommendations = recommender.generate_comprehensive_recommendations(health_report)
        formatted_output = recommender.format_ddl_output(recommendations)
        
        print(f"\nComprehensive recommendations:")
        print(formatted_output)
        
    except Exception as e:
        print(f"Error in example usage: {str(e)}")
