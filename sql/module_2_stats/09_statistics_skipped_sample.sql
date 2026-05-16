-- =============================================================================
-- Component   : Skipped and Sample Statistics
-- =============================================================================
-- Description : Identifies statistics being skipped (StatsSkipCount > 0) or using
--               sample (SampleSizePct between 0 and 100) on tables with high CPU
--               impact. Uses DBC.StatsV, DBC.TablesV, and PDCRINFO.DBQLObjTbl_Hst
--               to correlate stats issues with actual CPU consumption. Prioritizes
--               tables by Total_Impact_CPU for remediation. Excludes system databases.
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Modificado  : 2026-05-13 - Integración y optimización de motor SQL para Módulo 2
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================


WITH Tablas_Con_Datos AS (
    -- Validamos que la tabla tenga espacio físico (datos reales)
    SELECT DatabaseName, TableName
    FROM DBC.TableSizeV
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 0 
),
Impacto_CPU_Tablas AS (
    -- Cálculo del "Total Impact CPU" a 30 días para atrapar procesos mensuales
    SELECT 
        o.ObjectDatabaseName,
        o.ObjectTableName,
        SUM(q.AMPCPUTime + q.ParserCPUTime) AS Total_Impact_CPU
    FROM PDCRINFO.DBQLObjTbl_Hst o
    INNER JOIN PDCRINFO.DBQLogTbl_Hst q
        ON o.LogDate = q.LogDate
        AND o.QueryID = q.QueryID
    WHERE o.LogDate BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE 
      AND o.ObjectType = 'Tab'
      AND o.ObjectDatabaseName NOT IN (
          'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
          'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
          'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
          'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
      )
	  
    GROUP BY 1, 2
)
SELECT DISTINCT 
    s.DatabaseName                                          AS DatabaseName, 
    s.TableName                                             AS TableName,
    TRIM(s.ColumnName)                                      AS ObjectName,
    'Skipped/Sample Stats'                                  AS FindingCategory,
    s.LastCollectTimeStamp                                   AS LastCollectTimeStamp,
    'COLLECT STATISTICS COLUMN (' || TRIM(s.ColumnName) || ') ON ' || TRIM(s.DatabaseName) || '.' || TRIM(s.TableName) || ';' AS Action_SQL
FROM DBC.StatsV s
INNER JOIN DBC.TablesV t 
    ON s.DatabaseName = t.DatabaseName 
    AND s.TableName = t.TableName
INNER JOIN Tablas_Con_Datos td
    ON s.DatabaseName = td.DatabaseName
    AND s.TableName = td.TableName
LEFT JOIN Impacto_CPU_Tablas c
    ON s.DatabaseName = c.ObjectDatabaseName
    AND s.TableName = c.ObjectTableName
WHERE t.TableKind = 'T'
  AND s.StatsId <> 0 
  AND CAST(s.LastCollectTimeStamp AS DATE) < CURRENT_DATE - 14 
  AND (
        (s.SampleSizePct > 0 AND s.SampleSizePct < 100) 
        OR s.StatsSkipCount > 0
  )
  AND s.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
  and ObjectName not like'%CASE%'
ORDER BY COALESCE(c.Total_Impact_CPU, 0) DESC, s.DatabaseName, s.TableName;