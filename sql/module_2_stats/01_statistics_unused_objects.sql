-- =============================================================================
-- Component 1: Unused Objects - Statistics Collection Without Table Usage
-- =============================================================================
-- Description : Identifies tables with recent statistics collection (last 30 days)
--               but no real usage activity in PDCRINFO.DBQLObjTbl_Hst (90-day
--               window), suggesting wasted CPU on COLLECT STATS.
--               Excludes: DBC databases, SUMMARY stats (StatsId = 0), stats
--               defined over the Primary Index (optimizer uses these for AMP
--               redistribution even on direct hash access, which does not
--               generate DML records in DBQL), and any activity originated by
--               STATSMANAGER or Collect/Dump Statistics statements.
--
-- Version     : 1.0.0
-- Date        : 2025-04-29
-- Modificado  : 2026-05-27 - v3: replaced DBC.ObjectUsage with PDCRINFO.DBQLObjTbl_Hst
--                             as source of truth for real table access. DBC.ObjectUsage
--                             does not reliably capture access via CTAS, views, macros,
--                             or stored procedures. Added PI protection rule and
--                             STATSMANAGER/Collect Statistics exclusion from activity join.
--
-- Author      : Ricardo Enciso
-- Environment : Teradata 20 / PDCRINFO DBQL history enabled (90-day retention)
--
-- Prerequisites:
--   - PDCRINFO.DBQLObjTbl_Hst populated with ObjectType = 'Tab'
--   - PDCRINFO.DBQLogTbl_Hst available for StatementType/UserName filtering
--   - DBC.IndicesV accessible for Primary Index column resolution
--
-- Changelog:
--   v1.0.0 - Initial version
--            - Fixed NULL logic in HAVING (Last_Actual_Access IS NULL propagation)
--            - Fixed join to DBC.ObjectUsage (uses DatabaseId/ObjectId, not names)
--            - Moved TableKind filter inside subquery for better performance
--            - Added DatabaseName <> 'DBC'
--            - Added StatsId <> 0 to exclude SUMMARY statistics
--            - Replaced DBC.TableSizeV direct TableKind usage with JOIN to DBC.TablesV
--            - LastAccessTimeStamp sourced from DBC.ObjectUsage base table
--
--   v3.0.0 - 2026-05-27
--            - Replaced DBC.ObjectUsage with PDCRINFO.DBQLObjTbl_Hst + DBQLogTbl_Hst
--              (fixes false positives from tables accessed via CTAS, views, macros)
--            - Added PI protection: NOT EXISTS on DBC.IndicesV excludes stats whose
--              columns match the table Primary Index (IndexType IN ('P','Q'))
--            - Excluded STATSMANAGER user and StatementType IN ('Collect Statistics',
--              'Dump

SELECT
    s.DatabaseName,
    s.TableName,
    CAST(SUM(t.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    MAX(CAST(s.LastCollectTimeStamp AS DATE))                AS Last_Stat_Collect,
    MAX(obj.LogDate)                                         AS Last_DBQL_Access,
    'DROP STATISTICS ON '
        || TRIM(s.DatabaseName) || '.' || TRIM(s.TableName)
        || ';'                                               AS Action_SQL
FROM DBC.StatsV s

INNER JOIN (
    SELECT
        ts.DatabaseName,
        ts.TableName,
        SUM(ts.CurrentPerm) AS CurrentPerm
    FROM DBC.TableSizeV ts
    JOIN DBC.TablesV tb
        ON  ts.DatabaseName = tb.DatabaseName
        AND ts.TableName    = tb.TableName
    WHERE tb.TableKind = 'T'
      AND tb.AuthName  IS NULL
    GROUP BY ts.DatabaseName, ts.TableName
) t
    ON  s.DatabaseName = t.DatabaseName
    AND s.TableName    = t.TableName

-- Actividad real en DBQL: cualquier tipo de operación sobre la tabla
LEFT JOIN (
    SELECT
        ObjectDatabaseName,
        ObjectTableName,
        MAX(LogDate) AS LogDate
    FROM PDCRINFO.DBQLObjTbl_Hst
    WHERE LogDate    >= CURRENT_DATE - 90
      AND ObjectType  = 'Tab'
    GROUP BY ObjectDatabaseName, ObjectTableName
) obj
    ON  s.DatabaseName = obj.ObjectDatabaseName
    AND s.TableName    = obj.ObjectTableName

-- Protección: excluir stats sobre Primary Index
WHERE NOT EXISTS (
    SELECT 1
    FROM DBC.IndicesV pi
    WHERE pi.DatabaseName = s.DatabaseName
      AND pi.TableName    = s.TableName
      AND pi.IndexType   IN ('P','Q')
      AND pi.ColumnName   = s.ColumnName
)
  AND CAST(s.LastCollectTimeStamp AS DATE) >= CURRENT_DATE - 30
  AND s.DatabaseName NOT IN (
          'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
          'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
          'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
          'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
      )
  AND s.StatsId       <> 0

GROUP BY s.DatabaseName, s.TableName

-- Solo tablas sin ninguna actividad en DBQL en 90 días
HAVING MAX(obj.LogDate) IS NULL

ORDER BY Size_GB DESC;