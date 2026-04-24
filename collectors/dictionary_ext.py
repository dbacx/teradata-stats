"""
Dictionary Metadata Extractor for Teradata Statistics

This module provides efficient extraction of database statistics metadata from Teradata
system tables, joining DBC.StatsV, DBC.TablesV, and DBC.TableSizeV with proper
aggregation to avoid Cartesian products and ensure accurate space calculations.
"""

import logging
import pandas as pd
from typing import Optional
from core.connection import create_connection

# Configure logging
logger = logging.getLogger(__name__)


def extract_database_stats(database_name: str) -> pd.DataFrame:
    """
    Extract comprehensive database statistics metadata from Teradata system tables.
    
    This function performs an optimized join between DBC.StatsV, DBC.TablesV,
    and a pre-aggregated DBC.TableSizeV to avoid Cartesian products and ensure
    accurate space calculations.
    
    Args:
        database_name: Name of the database to extract statistics from
        
    Returns:
        DataFrame containing statistics metadata with columns:
        - DatabaseName
        - TableName
        - ColumnName
        - IndexName
        - StatisticsType
        - CreateTime
        - LastCollectTimeStamp
        - Version
        - TableSizeGB
        - TableKind
        
    Raises:
        ValueError: If database_name is empty or None
        Exception: If SQL execution fails
    """
    if not database_name or not database_name.strip():
        raise ValueError("Database name cannot be empty or None")
    
    conn = None
    try:
        # Create connection using the core module
        td_conn = create_connection()
        conn = td_conn.connect()
        
        # Optimized SQL query with pre-aggregated TableSizeV to avoid Cartesian products
        sql_query = f"""
        WITH TableSize_Aggregated AS (
            SELECT 
                DatabaseName,
                TableName,
                SUM(CurrentPerm) AS TotalCurrentPerm
            FROM DBC.TableSizeV
            WHERE DatabaseName = '{database_name}'
            GROUP BY DatabaseName, TableName
        )
        SELECT 
            s.DatabaseName,
            s.TableName,
            s.ColumnName,
            s.IndexName,
            s.StatisticsType,
            s.CreateTime,
            s.LastCollectTimeStamp,
            s.Version,
            -- Convert bytes to GB within SQL for efficiency
            CAST(COALESCE(ts.TotalCurrentPerm, 0) AS BIGINT) / 1024.0 / 1024.0 / 1024.0 AS TableSizeGB,
            t.TableKind
        FROM DBC.StatsV s
        INNER JOIN DBC.TablesV t ON 
            s.DatabaseName = t.DatabaseName AND 
            s.TableName = t.TableName
        LEFT JOIN TableSize_Aggregated ts ON 
            s.DatabaseName = ts.DatabaseName AND 
            s.TableName = ts.TableName
        WHERE s.DatabaseName = '{database_name}'
            AND t.TableKind = 'T'  -- Filter only tables (not views, macros, etc.)
        ORDER BY s.DatabaseName, s.TableName, s.ColumnName
        """
        
        # Execute query using pandas for efficient DataFrame creation
        logger.info(f"Extracting statistics metadata for database: {database_name}")
        
        df = pd.read_sql(sql_query, conn)
        
        # Log successful extraction
        row_count = len(df)
        logger.info(f"Successfully extracted {row_count} statistics records for database '{database_name}'")
        
        # Basic data validation
        if row_count == 0:
            logger.warning(f"No statistics records found for database '{database_name}'")
        
        # Convert timestamp columns to datetime if they exist
        timestamp_columns = ['CreateTime', 'LastCollectTimeStamp']
        for col in timestamp_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
        
    except Exception as e:
        error_msg = f"Failed to extract statistics for database '{database_name}': {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
        
    finally:
        # Ensure connection is always closed
        if conn:
            try:
                conn.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing database connection: {str(e)}")


def extract_database_stats_batch(database_names: list) -> pd.DataFrame:
    """
    Extract statistics metadata for multiple databases in batch.
    
    Args:
        database_names: List of database names to extract statistics from
        
    Returns:
        Combined DataFrame with statistics from all specified databases
    """
    if not database_names:
        raise ValueError("Database names list cannot be empty")
    
    all_dfs = []
    
    for db_name in database_names:
        try:
            df = extract_database_stats(db_name)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.warning(f"Failed to extract stats for database '{db_name}': {str(e)}")
            continue
    
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Combined statistics extracted: {len(combined_df)} total records from {len(all_dfs)} databases")
        return combined_df
    else:
        logger.warning("No statistics extracted from any database")
        return pd.DataFrame()


def validate_database_access(database_name: str) -> bool:
    """
    Validate if the specified database exists and is accessible.
    
    Args:
        database_name: Database name to validate
        
    Returns:
        True if database exists and is accessible, False otherwise
    """
    try:
        td_conn = create_connection()
        conn = td_conn.connect()
        
        query = f"SELECT 1 FROM DBC.Databases WHERE DatabaseName = '{database_name}'"
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return result is not None
        
    except Exception as e:
        logger.error(f"Database access validation failed for '{database_name}': {str(e)}")
        return False


if __name__ == "__main__":
    # Example usage
    try:
        # Test with a sample database name
        test_db = "DBC"  # DBC database should always exist
        logger.info(f"Testing extraction for database: {test_db}")
        
        if validate_database_access(test_db):
            stats_df = extract_database_stats(test_db)
            
            if not stats_df.empty:
                print(f"Extracted {len(stats_df)} statistics records")
                print("\nSample data:")
                print(stats_df.head())
                print(f"\nColumns: {list(stats_df.columns)}")
            else:
                print("No statistics found")
        else:
            print(f"Database '{test_db}' not accessible")
            
    except Exception as e:
        print(f"Error: {str(e)}")
