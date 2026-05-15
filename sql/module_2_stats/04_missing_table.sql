-- =============================================================================
-- Component   : Missing at Table Level
-- =============================================================================
-- Description : Identifies actively used tables (accessed yesterday) without any
--               statistics. Uses PDCRINFO.DBQLObjTbl_Hst to detect table usage
--               and DBC.StatsV to check for missing stats. Critical for tables
--               with recent activity but no statistics for optimization. Excludes
--               system databases.
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Modificado  : 2026-05-13 - Integración y optimización de motor SQL para Módulo 2
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Tablas_Con_Datos AS (
    SELECT DatabaseName, TableName
    FROM DBC.TableSizeV
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 0 
)
SELECT DISTINCT 
    o.ObjectDatabaseName AS DatabaseName, 
    o.ObjectTableName AS TableName,
    t.TableKind,
    'COLLECT SUMMARY STATISTICS ON ' || TRIM(o.ObjectDatabaseName) || '.' || TRIM(o.ObjectTableName) || ';' AS Action_SQL
FROM PDCRINFO.DBQLObjTbl_Hst o
INNER JOIN DBC.TablesV t 
    ON o.ObjectDatabaseName = t.DatabaseName 
    AND o.ObjectTableName = t.TableName
INNER JOIN Tablas_Con_Datos td
    ON o.ObjectDatabaseName = td.DatabaseName
    AND o.ObjectTableName = td.TableName
LEFT JOIN DBC.StatsV s 
    ON o.ObjectDatabaseName = s.DatabaseName 
    AND o.ObjectTableName = s.TableName
WHERE o.LogDate = CURRENT_DATE - 1 
  AND t.TableKind = 'T' 
  AND o.ObjectType = 'Tab' 
  AND s.TableName IS NULL
  AND o.ObjectDatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
ORDER BY 1, 2;
