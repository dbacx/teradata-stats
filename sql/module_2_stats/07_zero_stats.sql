-- =============================================================================
-- Component   : Zero Statistics
-- =============================================================================
-- Description : Identifies statistics with RowCount=0 on tables that actually
--               contain data (physical space > 0). Uses DBC.StatsV and
--               DBC.TableSizeV to detect critical statistics issues that can cause
--               Product Join disasters. Excludes SUMMARY stats and system databases.
--               CRITICAL severity for optimizer accuracy.
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Modificado  : 2026-05-13 - Integración y optimización de motor SQL para Módulo 2
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Tamaño_Tablas AS (
    SELECT 
        ts.DatabaseName, 
        ts.TableName, 
        SUM(ts.CurrentPerm) AS CurrentPerm
    FROM DBC.TableSizeV ts
    INNER JOIN DBC.TablesV tb 
        ON ts.DatabaseName = tb.DatabaseName 
       AND ts.TableName    = tb.TableName
    WHERE tb.TableKind = 'T'
    GROUP BY 1, 2
)
SELECT 
    S.DatabaseName, 
    S.TableName, 
    CAST(T.CurrentPerm / (1024.0**3) AS DECIMAL(18,4)) AS Size_GB,
    MIN(S.RowCount)                                    AS Stats_RowCount,
    CAST(MAX(S.LastCollectTimeStamp) AS DATE)          AS Ultima_Recoleccion,
    'COLLECT STATISTICS ON ' || TRIM(S.DatabaseName) || '.' || TRIM(S.TableName) || ';' AS Action_SQL
FROM DBC.StatsV S
INNER JOIN Tamaño_Tablas T 
    ON S.DatabaseName = T.DatabaseName 
   AND S.TableName    = T.TableName
WHERE S.RowCount = 0
  AND S.StatsId <> 0
  AND S.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA'
  )
GROUP BY 
    S.DatabaseName, 
    S.TableName, 
    Size_GB
ORDER BY 
    Size_GB DESC, 
    S.DatabaseName, 
    S.TableName;

