"""
TD Stats Optimizer - CLI Entry Point

Simple command-line interface for running statistics analysis without the web UI.
This provides an alternative entry point for automated or headless execution.
"""

import sys
import os
import logging
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from collectors.dictionary_ext import extract_database_stats
from analyzers.health_rules import StatsAnalyzer
from skills.recommender import DDLRecommender

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main CLI execution function."""
    print("📊 TD Stats Optimizer - CLI Mode")
    print("=" * 50)
    
    # Get database name from user input
    database_name = input("Enter database name to analyze: ").strip()
    
    if not database_name:
        print("❌ Error: Database name cannot be empty")
        return
    
    try:
        print(f"\n🔄 Starting analysis for database: {database_name}")
        
        # Step 1: Extract statistics metadata
        print("📋 Extracting dictionary metadata...")
        df_stats = extract_database_stats(database_name)
        
        if df_stats.empty:
            print(f"❌ No statistics found for database '{database_name}'")
            return
        
        print(f"✅ Extracted {len(df_stats)} statistics records")
        
        # Step 2: Analyze statistics health
        print("\n🔍 Analyzing statistics health...")
        analyzer = StatsAnalyzer()
        
        stale_stats = analyzer.detect_stale_stats(df_stats, days_threshold=15)
        bloat_analysis = analyzer.detect_dictionary_bloat(df_stats, max_stats_per_table=50)
        
        print(f"✅ Found {len(stale_stats)} stale statistics")
        print(f"✅ Found {len(bloat_analysis)} tables with potential bloat")
        
        # Step 3: Generate DDL recommendations
        print("\n🔧 Generating DDL recommendations...")
        recommender = DDLRecommender()
        
        collect_ddls = recommender.generate_collect_stats(stale_stats)
        drop_ddls = recommender.generate_drop_stats(bloat_analysis)
        
        print(f"✅ Generated {len(collect_ddls)} COLLECT STATISTICS statements")
        print(f"✅ Generated {len(drop_ddls)} DROP STATISTICS statements")
        
        # Step 4: Save recommendations to files
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if collect_ddls:
            collect_file = f"collect_stats_{database_name}_{timestamp}.sql"
            with open(collect_file, 'w') as f:
                f.write("-- COLLECT STATISTICS for stale data\n")
                f.write(f"-- Database: {database_name}\n")
                f.write(f"-- Generated: {datetime.now()}\n")
                f.write("--\n")
                f.write("\n".join(collect_ddls))
            print(f"📁 Saved COLLECT statements to: {collect_file}")
        
        if drop_ddls:
            drop_file = f"drop_stats_{database_name}_{timestamp}.sql"
            with open(drop_file, 'w') as f:
                f.write("-- DROP STATISTICS for dictionary bloat cleanup\n")
                f.write(f"-- Database: {database_name}\n")
                f.write(f"-- Generated: {datetime.now()}\n")
                f.write("--\n")
                f.write("\n".join(drop_ddls))
            print(f"📁 Saved DROP statements to: {drop_file}")
        
        # Summary
        print("\n" + "=" * 50)
        print("📊 ANALYSIS SUMMARY")
        print("=" * 50)
        print(f"Database: {database_name}")
        print(f"Total Tables: {df_stats['TableName'].nunique()}")
        print(f"Total Statistics: {len(df_stats)}")
        print(f"Stale Statistics: {len(stale_stats)}")
        print(f"Tables with Bloat: {len(bloat_analysis)}")
        print(f"DDL Files Generated: {len(collect_ddls) > 0} + {len(drop_ddls) > 0}")
        print("=" * 50)
        print("✅ Analysis completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        logger.error(f"CLI analysis error: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
