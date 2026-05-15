-- =============================================================================
-- Component   : Missing at PARTITION level
-- =============================================================================
-- Description : Identifies PPI (Partitioned Primary Index) tables without PARTITION
--               column statistics. Uses DBC.IndexConstraints to detect PPI tables
--               and DBC.StatsV to check for missing PARTITION-level stats. Critical
--               for query optimization on partitioned tables. Excludes system databases.
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
    ic.DatabaseName, 
    ic.TableName,
    t.PartitioningLevels,
    'COLLECT STATISTICS COLUMN PARTITION ON ' || TRIM(ic.DatabaseName) || '.' || TRIM(ic.TableName) || ';' AS Action_SQL
FROM DBC.IndexConstraints ic
INNER JOIN DBC.TablesV t 
    ON ic.DatabaseName = t.DatabaseName 
    AND ic.TableName = t.TableName
INNER JOIN Tablas_Con_Datos td
    ON ic.DatabaseName = td.DatabaseName
    AND ic.TableName = td.TableName
LEFT JOIN DBC.StatsV s 
    ON ic.DatabaseName = s.DatabaseName 
    AND ic.TableName = s.TableName 
    AND UPPER(s.ColumnName) = 'PARTITION'
WHERE ic.ConstraintType = 'Q' 
  AND s.ColumnName IS NULL
  AND t.PartitioningLevels > 0
  AND ic.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
ORDER BY ic.DatabaseName, ic.TableName;
