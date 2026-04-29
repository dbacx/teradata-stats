"""
Rule 16: Urgent Missing Statistics Detection

This rule identifies tables with high CPU usage, high frequency, large size (>10GB)
that have NO statistics at all, and generates ready-to-use COLLECT DDL statements.
Based on SQL_DBA_STATS_Urgentes.txt logic.
"""

import logging
import pandas as pd
from typing import Dict, Any
from analyzers.base_rule import BaseStatsRule
from collectors.dbql_ext import extract_urgent_missing_stats

logger = logging.getLogger(__name__)


class Rule16UrgentMissing(BaseStatsRule):
    """
    Rule to detect urgent missing statistics on high-impact tables.
    
    Identifies tables with TotalCPU >= 1000, FreqOfUse >= 20, TableSize >= 10GB
    that have NO statistics in StatsV, and generates ready-to-use COLLECT DDL.
    """
    
    def __init__(self, min_cpu: float = 1000.0, min_freq: int = 20, min_size_gb: float = 10.0, days_back: int = 30):
        super().__init__(
            rule_id="rule_16_urgent_missing",
            rule_name="Urgent Missing Stats",
            description="Detects high-impact tables without stats and generates COLLECT DDL"
        )
        self.min_cpu = min_cpu
        self.min_freq = min_freq
        self.min_size_gb = min_size_gb
        self.days_back = days_back
        logger.info(f"Initialized Rule16UrgentMissing with CPU>={min_cpu}, Freq>={min_freq}, Size>={min_size_gb}GB")
    
    def analyze(self, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze DBQL and dictionary data to find urgent missing stats.
        
        Args:
            context: Dictionary containing DataFrames for analysis.
                    Expected keys: 'stats_df' (optional, for comparison)
        
        Returns:
            DataFrame containing urgent tables with COLLECT DDL statements
        """
        # Stats df is optional for this rule since it queries DBQL directly
        stats_df = context.get('stats_df', pd.DataFrame())
        
        self.log_analysis_start(context)
        
        try:
            # Extract urgent missing stats from DBQL
            logger.info(f"Extracting urgent missing stats from DBQL (last {self.days_back} days)")
            urgent_df = extract_urgent_missing_stats(
                min_cpu=self.min_cpu,
                min_freq=self.min_freq,
                min_size_gb=self.min_size_gb,
                days_back=self.days_back
            )
            
            if urgent_df.empty:
                logger.info("No urgent missing stats found")
                return urgent_df
            
            # Add analysis-specific columns
            urgent_df['recommendation'] = 'COLLECT STATISTICS USING NO THRESHOLD'
            urgent_df['reason'] = (
                urgent_df.apply(
                    lambda row: f"High-impact table: CPU={row['TotalCPU']:.0f}, Freq={row['FreqOfUse']}, Size={row['TableSize']/1024**3:.2f}GB, NO STATS",
                    axis=1
                )
            )
            
            result_df = self.add_metadata_columns(urgent_df)
            
            logger.info(f"Found {len(result_df)} urgent tables missing statistics")
            
            self.log_analysis_end(result_df)
            return result_df
            
        except Exception as e:
            logger.error(f"Error in Rule16UrgentMissing analysis: {str(e)}")
            raise
    
    def get_urgent_ddl(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract the COLLECT DDL statements from the results.
        
        Args:
            result_df: DataFrame returned by analyze method
        
        Returns:
            DataFrame with DDL statements
        """
        if result_df.empty or 'Comando_Collect' not in result_df.columns:
            return pd.DataFrame()
        
        ddl_df = result_df[['DatabaseName', 'TableName', 'FreqOfUse', 'TotalCPU', 
                            'TableSize', 'Comando_Collect']].copy()
        ddl_df = ddl_df.rename(columns={'Comando_Collect': 'DDL_Statement'})
        
        return ddl_df
    
    def get_top_urgent_tables(self, result_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
        """
        Get top N most urgent tables by CPU impact.
        
        Args:
            result_df: DataFrame returned by analyze method
            top_n: Number of top tables to return
        
        Returns:
            DataFrame with most urgent tables
        """
        if result_df.empty:
            return pd.DataFrame()
        
        top_urgent = (
            result_df
            .sort_values('TotalCPU', ascending=False)
            .head(top_n)
            .copy()
        )
        
        return top_urgent


if __name__ == "__main__":
    try:
        # This rule requires actual DBQL connection for testing
        # For now, just test instantiation
        rule = Rule16UrgentMissing()
        print(f"Rule instantiated: {rule.rule_id} - {rule.rule_name}")
        
        # Test with empty context
        context = {'stats_df': pd.DataFrame()}
        result = rule.analyze(context)
        print(f"Result: {len(result)} urgent tables")
        
    except Exception as e:
        print(f"Error: {str(e)}")
