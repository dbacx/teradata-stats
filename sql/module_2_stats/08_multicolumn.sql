-- =============================================================================
-- Component   : Multicolumn MaxValueLength
-- =============================================================================
-- Description : Identifies multicolumn statistics (ExpressionCount > 1) with
--               MaxValueLength <= 25, suggesting potential truncation issues.
--               Uses DBC.StatsV and DBC.TablesV to detect multicolumn stats that
--               may need USING MAXVALUELENGTH adjustment. Excludes system databases
--               and SUMMARY stats. LOW severity optimization opportunity.
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
    s.DatabaseName                                          AS DatabaseName, 
    s.TableName                                             AS TableName,
    TRIM(s.ColumnName)                                      AS ObjectName,
    'Multicolumn MaxValueLength'                            AS FindingCategory,
    s.LastCollectTimeStamp                                   AS LastCollectTimeStamp,
    'COLLECT STATISTICS USING MAXVALUELENGTH ' || TRIM(CAST(s.MaxValueLength * 2 AS VARCHAR(10))) || ' ON ' || TRIM(s.DatabaseName) || '.' || TRIM(s.TableName) || ' COLUMN (' || TRIM(s.ColumnName) || ');' AS RemediationDDL
FROM DBC.StatsV s
INNER JOIN DBC.TablesV t 
    ON s.DatabaseName = t.DatabaseName 
    AND s.TableName = t.TableName
INNER JOIN Tablas_Con_Datos td
    ON s.DatabaseName = td.DatabaseName
    AND s.TableName = td.TableName
WHERE s.ExpressionCount > 1 
  AND s.MaxValueLength <= 25
  AND t.TableKind = 'T'
  AND s.StatsId <> 0
  AND s.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
ORDER BY 1, 2, 3;