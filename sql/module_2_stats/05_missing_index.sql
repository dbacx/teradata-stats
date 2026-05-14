-- =============================================================================
-- Component   : Missing at Index Level
-- =============================================================================
-- Description : Identifies indexes (PI, SI, JI, PPI) without statistics. Uses
--               DBC.IndicesV to detect all index types and DBC.StatsV to check
--               for missing index-level statistics. Critical for query optimization
--               on indexed access paths. Excludes system databases.
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
    i.DatabaseName                                          AS DatabaseName, 
    i.TableName                                             AS TableName,
    COALESCE(TRIM(i.IndexName), 'INDEX #' || TRIM(CAST(i.IndexNumber AS VARCHAR(10)))) AS ObjectName,
    'Missing Index Stats'                                   AS FindingCategory,
    CAST(NULL AS TIMESTAMP(0))                              AS LastCollectTimeStamp,
    'COLLECT STATISTICS INDEX (' || COALESCE(TRIM(i.IndexName), TRIM(CAST(i.IndexNumber AS VARCHAR(10)))) || ') ON ' || TRIM(i.DatabaseName) || '.' || TRIM(i.TableName) || ';' AS RemediationDDL
FROM DBC.IndicesV i
INNER JOIN Tablas_Con_Datos t
    ON i.DatabaseName = t.DatabaseName
    AND i.TableName = t.TableName
LEFT JOIN DBC.StatsV s 
    ON i.DatabaseName = s.DatabaseName 
    AND i.TableName = s.TableName 
    AND i.IndexNumber = s.IndexNumber
WHERE i.IndexType IN ('P', 'S', 'Q', 'J') 
  AND s.IndexNumber IS NULL
  AND i.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
ORDER BY i.DatabaseName, i.TableName, i.IndexType;