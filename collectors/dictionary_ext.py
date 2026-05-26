# DEPRECATED — Legacy module kept for backward compatibility with ui/app.py.
# New modules should use the modular collectors in collectors/mod*_collector.py instead.
"""
Dictionary Metadata Extractor for Teradata Statistics

This module provides efficient extraction of database statistics metadata from Teradata
system tables, joining DBC.StatsV, DBC.TablesV, and DBC.TableSizeV with proper
aggregation to avoid Cartesian products and ensure accurate space calculations.
"""

import logging
import pandas as pd
from typing import Optional
from core.connection import create_connection_from_params
from core.schemas import validate_columns

# Configure logging
logger = logging.getLogger(__name__)


def _get_connection():
    """Obtain a live Teradata connection from session-state params."""
    try:
        import streamlit as st
        params = st.session_state.get("td_params", {})
        if params:
            return create_connection_from_params(params)
    except Exception:
        pass
    raise ConnectionError("No connection parameters available. Connect via sidebar first.")


def extract_database_stats(database_name: Optional[str] = None, table_name: Optional[str] = None) -> pd.DataFrame:
    """
    Extract comprehensive database statistics metadata from Teradata system tables.
    
    Args:
        database_name: Name of the database to extract statistics from. If None or "ALL", extracts from all databases (excluding system databases).
        table_name: Name of the specific table to extract. If provided, filters by table name.
        
    Returns:
        DataFrame containing statistics metadata with official Teradata DBC columns.
        
    Raises:
        ValueError: If database_name is empty string (but None or "ALL" are valid)
        Exception: If SQL execution fails
    """
    # System databases to exclude
    system_databases = [
        'ALL', 'CONSOLE', 'CRASHDUMPS', 'DBA', 'DBC', 'DBCEXTENSION', 'DBCMANAGER',
        'DBCMNGR', 'dbcmngr12', 'dbqm', 'dbqrymgr', 'DEFAULT', 'EXTUser', 'HIGA',
        'LockLogShredder', 'NETVAULT1', '$NETVAULT_CATALOG', 'PDCRACCESS',
        'PDCRADM', 'PDCRADMIN', 'PDCRCANARY0M', 'PDCRCANARY1M', 'PDCRCANARY2M',
        'PDCRCANARY3M', 'PDCRCANARY4M', 'PDCRHIGA', 'PDCRINFO', 'PDCRSTG',
        'PDCRTPCD', 'PMCPAccess', 'PMCPADM', 'PMCPADMIN', 'pmcpawt',
        'PMCPCANARYAPPL', 'PMCPCANARYLOAD', 'PMCPCANARYUSER', 'PMCPHIGA',
        'PMCPINFO', 'PMCPTPCD', 'PUBLIC', 'qcd', 'spoolreserve', 'spool_reserve',
        'SQLJ', 'stats', 'SYSADMIN', 'SYSBAR', 'SYS_CALENDAR', 'SYSJDBC', 'SYSLIB',
        'SYS_MGMT', 'SYSSPATIAL', 'SYSTEMFE', 'SYSUDTLIB', 'SYSUIF', 'SYSUSR',
        'SYSXML', 'TDMaps', 'TDPUSER', 'TDQCD', 'TD_SERVER_DB', 'TDStats',
        'TD_SYSFNLIB', 'TD_SYSXML', 'TDWM', 'TMADMIN', 'tswiz', 'tswizdbase',
        'twm', 'twm_results', 'twm_source', 'VIEWPOINT', 'XASF_FAST_PATH'
    ]
    
    conn = None
    try:
        conn = _get_connection()
        
        # Build dynamic WHERE clause based on parameters
        where_conditions = []
        tablesize_where_conditions = []
        
        # Database filter
        if database_name and database_name.strip() and database_name.upper() != "ALL":
            where_conditions.append(f"s.DatabaseName = '{database_name}'")
            tablesize_where_conditions.append(f"DatabaseName = '{database_name}'")
        else:
            # Exclude system databases when doing system-wide extraction
            system_db_filter = ", ".join([f"'{db}'" for db in system_databases])
            where_conditions.append(f"s.DatabaseName NOT IN ({system_db_filter})")
            where_conditions.append("s.DatabaseName NOT LIKE ALL('%PDCR%', '%PMCP%', '%QCD%', '%tdwm%', '%tswiz%', '%twm%', 'ADLS%')")
            tablesize_where_conditions.append(f"DatabaseName NOT IN ({system_db_filter})")
        
        # Table filter
        if table_name and table_name.strip():
            where_conditions.append(f"s.TableName = '{table_name}'")
            tablesize_where_conditions.append(f"TableName = '{table_name}'")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        tablesize_where_clause = " AND ".join(tablesize_where_conditions) if tablesize_where_conditions else "1=1"
        
        # Optimized SQL query with pre-aggregated TableSizeV to avoid Cartesian products
        # Using exact column names from DBC.StatsV, DBC.TablesV, and DBC.TableSizeV as defined in schemas.py
        sql_query = f"""
        WITH TableSize_Aggregated AS (
            SELECT 
                DatabaseName,
                TableName,
                SUM(CurrentPerm) AS TotalCurrentPerm
            FROM DBC.TableSizeV
            WHERE {tablesize_where_clause}
            GROUP BY DatabaseName, TableName
        )
        SELECT 
            s.DatabaseName,
            s.TableName,
            s.ColumnName,
            s.StatsName,
            s.StatsType,
            s.StatsSource,
            s.ValidStats,
            s.RowCount,
            s.UniqueValueCount,
            s.NullCount,
            s.CreateTimeStamp,
            s.LastCollectTimeStamp,
            s.LastAlterTimeStamp,
            -- Convert bytes to GB within SQL for efficiency
            CAST(COALESCE(ts.TotalCurrentPerm, 0) AS BIGINT) / 1024.0 / 1024.0 / 1024.0 AS TableSizeGB,
            t.TableKind,
            t.AccessCount,
            t.LastAccessTimeStamp
        FROM DBC.StatsV s
        INNER JOIN DBC.TablesV t ON 
            s.DatabaseName = t.DatabaseName AND 
            s.TableName = t.TableName
        LEFT JOIN TableSize_Aggregated ts ON 
            s.DatabaseName = ts.DatabaseName AND 
            s.TableName = ts.TableName
        WHERE {where_clause}
            AND t.TableKind = 'T'  -- Filter only tables (not views, macros, etc.)
        ORDER BY s.DatabaseName, s.TableName, s.ColumnName
        """
        
        scope = f"database: {database_name}" if database_name and database_name.upper() != "ALL" else "entire system (excluding system databases)"
        if table_name:
            scope += f", table: {table_name}"
        logger.info(f"Extracting statistics metadata for {scope}")
        
        # Execute query using pandas for efficient DataFrame creation
        df = pd.read_sql(sql_query, conn)
        # Validate DataFrame columns against expected schema
        expected_columns = [
            'DatabaseName', 'TableName', 'ColumnName', 'StatsName', 'StatsType',
            'StatsSource', 'ValidStats', 'RowCount', 'UniqueValueCount', 'NullCount',
            'CreateTimeStamp', 'LastCollectTimeStamp', 'LastAlterTimeStamp',
            'TableSizeGB', 'TableKind', 'AccessCount', 'LastAccessTimeStamp'
        ]
        
        validate_columns(df, expected_columns, 'DBC.StatsV')
        
        # Log successful extraction
        row_count = len(df)
        logger.info(f"Successfully extracted {row_count} statistics records for database '{database_name}'")
        
        # Basic data validation
        if row_count == 0:
            logger.warning(f"No statistics records found for database '{database_name}'")
        
        # Convert timestamp columns to datetime if they exist
        timestamp_columns = ['CreateTimeStamp', 'LastCollectTimeStamp', 'LastAlterTimeStamp', 'LastAccessTimeStamp']
        for col in timestamp_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
        
    except Exception as e:
        error_msg = f"Failed to extract statistics for database '{database_name}': {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
        
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing database connection: {str(e)}")


def extract_database_stats_batch(database_names: list) -> pd.DataFrame:
    """Extract statistics metadata for multiple databases in batch."""
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
        logger.info(f"Combined statistics extracted: {len(combined_df)} records")
        return combined_df
    else:
        return pd.DataFrame()


def validate_database_access(database_name: str) -> bool:
    """Validate if the specified database exists and is accessible."""
    try:
        conn = _get_connection()

        query = "SELECT 1 FROM DBC.Databases WHERE DatabaseName = ?"
        cursor = conn.cursor()
        cursor.execute(query, [database_name])
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return result is not None
    except Exception as e:
        logger.error(f"Database access validation failed for '{database_name}': {str(e)}")
        return False