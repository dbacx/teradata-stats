# DEPRECATED — Legacy module kept for backward compatibility with analyzers/rules/rule_16_urgent_missing.py.
# New modules should use the modular collectors in collectors/mod*_collector.py instead.
"""
DBQL Metadata Extractor for Teradata Statistics

This module provides extraction of DBQL (Database Query Log) transactional data
from PDCRINFO.dbqlogtbl_hst and PDCRINFO.dbqlobjtbl_hst for analyzing table usage patterns.
"""

import logging
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta
from core.connection import create_connection

# Configure logging
logger = logging.getLogger(__name__)


def extract_dbql_usage(days_back: int = 30, database_name: Optional[str] = None, 
                      table_name: Optional[str] = None) -> pd.DataFrame:
    """
    Extract DBQL usage data for table activity analysis.
    
    Based on the temp_logtbl CTE from SQL_DBA_STATS_Urgentes.txt, this function
    extracts query activity metrics including CPU usage and frequency.
    
    Args:
        days_back: Number of days to look back for historical data (default: 30)
        database_name: Optional database filter. If None or "ALL", extracts from all databases.
        table_name: Optional table filter for specific table analysis.
        
    Returns:
        DataFrame containing DBQL usage metrics with columns:
        DatabaseName, TableName, FreqOfUse, TotalCPU
        
    Raises:
        ValueError: If days_back is invalid
        Exception: If SQL execution fails
    """
    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")
    
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
        # Create connection using the core module
        td_conn = create_connection()
        conn = td_conn.connect()
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Build dynamic WHERE clause based on parameters
        where_conditions = [
            f"a.logdate BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'",
            "b.objecttype = 'Tab'",
            "a.statementtype = 'Select'"
        ]
        
        # Database filter
        if database_name and database_name.strip() and database_name.upper() != "ALL":
            where_conditions.append(f"b.ObjectDatabaseName = '{database_name}'")
        else:
            # Exclude system databases
            system_db_filter = ", ".join([f"'{db}'" for db in system_databases])
            where_conditions.append(f"b.ObjectDatabaseName NOT IN ({system_db_filter})")
            where_conditions.append("b.ObjectDatabaseName NOT LIKE ALL('%PDCR%', '%PMCP%', '%QCD%', '%tdwm%', '%tswiz%', '%twm%', 'ADLS%')")
        
        # Table filter
        if table_name and table_name.strip():
            where_conditions.append(f"b.ObjectTableName = '{table_name}'")
        
        where_clause = " AND ".join(where_conditions)
        
        # SQL query based on temp_logtbl CTE from SQL_DBA_STATS_Urgentes.txt
        sql_query = f"""
        SELECT
            b.ObjectDatabaseName AS DatabaseName,
            b.ObjectTableName AS TableName,
            COUNT(a.queryid) AS FreqOfUse,
            SUM(a.AMPCPUTime + a.ParserCPUTime) AS TotalCPU
        FROM PDCRINFO.dbqlogtbl_hst a  
        INNER JOIN PDCRINFO.dbqlobjtbl_hst b   
            ON a.procid = b.procid  
            AND a.queryid = b.queryid  
            AND a.logdate = b.logdate   
        WHERE {where_clause}
        GROUP BY b.ObjectDatabaseName, b.ObjectTableName
        ORDER BY TotalCPU DESC
        """
        
        scope = f"last {days_back} days"
        if database_name and database_name.upper() != "ALL":
            scope += f", database: {database_name}"
        if table_name:
            scope += f", table: {table_name}"
        logger.info(f"Extracting DBQL usage data for {scope}")
        
        # Execute query using pandas
        df = pd.read_sql(sql_query, conn)
        
        # Log successful extraction
        row_count = len(df)
        logger.info(f"Successfully extracted {row_count} DBQL usage records")
        
        # Basic data validation
        if row_count == 0:
            logger.warning(f"No DBQL usage records found for the specified criteria")
        
        # Convert numeric columns
        numeric_columns = ['FreqOfUse', 'TotalCPU']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
        
    except Exception as e:
        error_msg = f"Failed to extract DBQL usage data: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
        
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing database connection: {str(e)}")


def extract_urgent_missing_stats(min_cpu: float = 1000.0, min_freq: int = 20, 
                                   min_size_gb: float = 10.0, days_back: int = 30) -> pd.DataFrame:
    """
    Extract tables that urgently need statistics collection.
    
    Based on the complete query from SQL_DBA_STATS_Urgentes.txt, this function
    identifies tables with high CPU usage, high frequency, large size, and NO statistics.
    
    Args:
        min_cpu: Minimum total CPU threshold (default: 1000)
        min_freq: Minimum frequency of use threshold (default: 20)
        min_size_gb: Minimum table size in GB (default: 10.0)
        days_back: Number of days to look back for DBQL data (default: 30)
        
    Returns:
        DataFrame containing urgent tables with columns:
        DatabaseName, TableName, FreqOfUse, TotalCPU, TableSize, Comando_Collect
        
    Raises:
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
        # Create connection using the core module
        td_conn = create_connection()
        conn = td_conn.connect()
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # System database filter for SQL
        system_db_filter = ", ".join([f"'{db}'" for db in system_databases])
        
        # Minimum size in bytes (10 GB = 10,737,418,240 bytes)
        min_size_bytes = int(min_size_gb * 1024 * 1024 * 1024)
        
        # Complete query from SQL_DBA_STATS_Urgentes.txt
        sql_query = f"""
        WITH Tamanio_Tabla AS (
            SELECT 
                DatabaseName, 
                TableName, 
                SUM(CurrentPerm) AS TableSize 
            FROM DBC.TableSizeV 
            GROUP BY 1, 2
            HAVING SUM(CurrentPerm) >= {min_size_bytes}
        ),
        Target_Tables AS (
            SELECT       
                a.DbName AS DatabaseName,
                a.TbName AS TableName,
                SUM(a.Counts) AS FreqOfUse,
                SUM(a.TotalCPU) AS TotalCPU  
            FROM (
                SELECT
                    b.ObjectDatabaseName AS DbName,
                    b.ObjectTableName AS TbName,
                    COUNT(a.queryid) OVER (PARTITION BY b.ObjectDatabaseName, b.ObjectTableName) AS Counts,
                    SUM(a.AMPCPUTime + a.ParserCPUTime) OVER (PARTITION BY b.ObjectDatabaseName, b.ObjectTableName) AS TotalCPU 
                FROM PDCRINFO.dbqlogtbl_hst a  
                INNER JOIN PDCRINFO.dbqlobjtbl_hst b   
                    ON a.procid = b.procid  
                    AND a.queryid = b.queryid  
                    AND a.logdate = b.logdate   
                    AND a.logdate BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
                WHERE b.objecttype = 'Tab' 
                  AND a.statementtype = 'Select' 
                  AND b.ObjectDatabaseName NOT IN ({system_db_filter})
                  AND b.ObjectDatabaseName NOT LIKE ALL('%PDCR%', '%PMCP%', '%QCD%', '%tdwm%', '%tswiz%', '%twm%', 'ADLS%')
                GROUP BY 1, 2, a.statementtype
            ) a   
            INNER JOIN DBC.TablesV T  
                ON a.DbName = T.DatabaseName    
                AND a.TbName = T.TableName
            WHERE T.TableKind IN('T', 'I') 
                AND T.DatabaseName NOT IN ({system_db_filter})
                AND T.DatabaseName NOT LIKE ALL('%PDCR%', '%PMCP%', '%QCD%', '%tdwm%', '%tswiz%', '%twm%', 'ADLS%')     
                AND NOT EXISTS (
                    SELECT 1 FROM DBC.StatsV S 
                    WHERE S.DatabaseName = a.DbName AND S.TableName = a.TbName
                )
            GROUP BY 1, 2
            HAVING SUM(a.TotalCPU) >= {min_cpu} 
               AND SUM(a.Counts) >= {min_freq}
        ),
        Columnas_Indice AS (
            SELECT 
                DatabaseName, 
                TableName, 
                ColumnName
            FROM DBC.IndicesV
            WHERE IndexType IN ('P', 'Q')
        )
        SELECT 
            t.DatabaseName,
            t.TableName,
            t.FreqOfUse,
            t.TotalCPU,
            sz.TableSize,
            'COLLECT STATISTICS USING NO THRESHOLD FOR CURRENT ' || 
            COALESCE(
                TRIM(TRAILING ',' FROM (XMLAGG(TRIM('COLUMN (' || i.ColumnName || ')') || ',' ORDER BY i.ColumnName)(VARCHAR(10000)))),
                'COLUMN (1)'
            ) || 
            ' ON ' || TRIM(t.DatabaseName) || '.' || TRIM(t.TableName) || ';' AS Comando_Collect
        FROM Target_Tables t
        INNER JOIN Tamanio_Tabla sz 
            ON t.DatabaseName = sz.DatabaseName 
            AND t.TableName = sz.TableName
        LEFT JOIN Columnas_Indice i 
            ON t.DatabaseName = i.DatabaseName 
            AND t.TableName = i.TableName
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY t.TotalCPU DESC
        """
        
        logger.info(f"Extracting urgent missing stats (CPU>={min_cpu}, Freq>={min_freq}, Size>={min_size_gb}GB)")
        
        # Execute query using pandas
        df = pd.read_sql(sql_query, conn)
        
        # Log successful extraction
        row_count = len(df)
        logger.info(f"Successfully extracted {row_count} urgent missing stats records")
        
        # Convert numeric columns
        numeric_columns = ['FreqOfUse', 'TotalCPU', 'TableSize']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
        
    except Exception as e:
        error_msg = f"Failed to extract urgent missing stats: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
        
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing database connection: {str(e)}")


if __name__ == "__main__":
    # Example usage for testing
    try:
        # Test DBQL usage extraction
        print("Testing DBQL usage extraction...")
        dbql_df = extract_dbql_usage(days_back=30)
        print(f"Extracted {len(dbql_df)} DBQL usage records")
        
        # Test urgent missing stats extraction
        print("\nTesting urgent missing stats extraction...")
        urgent_df = extract_urgent_missing_stats()
        print(f"Extracted {len(urgent_df)} urgent missing stats records")
        
    except Exception as e:
        print(f"Error in example usage: {str(e)}")
