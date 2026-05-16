-- =============================================================================
-- Component 1: Unused Objects - Statistics Collection Without Table Usage
-- =============================================================================
-- Description : Identifies tables with recent statistics collection (last 30 days)
--               but no actual usage from DBQL ObjectUsage, suggesting wasted CPU
--               on COLLECT STATS. Uses DBC.StatsV, DBC.TableSizeV, DBC.TablesV,
--               and DBC.ObjectUsage to correlate stats collection with actual
--               table access patterns. Excludes DBC databases and SUMMARY stats.
-- 
-- Version     : 1.0.0
-- Date        : 2025-04-29
-- Modificado  : 2026-05-13 - Integración y optimización de motor SQL para Módulo 2
-- Author      : Ricardo Enciso
-- Environment : Teradata 20 / DBQL ObjectUsage globally enabled
--
-- Prerequisites:
--   - DBQL ObjectUsage = TRUE (verified via DBC.DBQLRules)
--   - DBC.ObjectUsage populated (ObjectUseCount active at DBS level)
--
-- Changelog:
--   v1.0.0 - Initial version
--          - Fixed NULL logic in HAVING (Last_Actual_Access IS NULL propagation)
--          - Fixed join to DBC.ObjectUsage (uses DatabaseId/ObjectId, not names)
--          - Moved TableKind filter inside subquery for better performance
--          - Added DatabaseName <> 'DBC' (DBC stats not monitored per Teradata doc)
--          - Added StatsId <> 0 to exclude SUMMARY statistics (COLLECT SUMMARY STATS)
--          - Replaced DBC.TableSizeV direct TableKind usage (column not in view)
--            with JOIN to DBC.TablesV to resolve TableKind
--          - LastAccessTimeStamp sourced from DBC.ObjectUsage base table
--            (not available in any DBC view)
-- =============================================================================

SELECT 
    s.DatabaseName, 
    s.TableName,
    CAST(SUM(t.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    MAX(CAST(s.LastCollectTimeStamp AS DATE))                AS Last_Stat_Collect,
    MAX(u.LastAccessTimeStamp)                               AS Last_Actual_Access,
    'DROP STATISTICS ON ' || TRIM(s.DatabaseName) || '.' || TRIM(s.TableName) || ';' AS Action_SQL
FROM DBC.StatsV s
INNER JOIN (
    SELECT ts.DatabaseName, ts.TableName, SUM(ts.CurrentPerm) AS CurrentPerm
    FROM DBC.TableSizeV ts
    JOIN DBC.TablesV tb 
        ON ts.DatabaseName = tb.DatabaseName 
        AND ts.TableName   = tb.TableName
    WHERE tb.TableKind = 'T'
	AND tb.AuthName is null
    GROUP BY 1, 2
) t ON s.DatabaseName = t.DatabaseName 
   AND s.TableName    = t.TableName
LEFT JOIN (
    SELECT 
        db.DatabaseName,
        tv.TVMName          AS ObjectName,
        ou.LastAccessTimeStamp
    FROM DBC.ObjectUsage ou
    JOIN DBC.Dbase db ON ou.DatabaseId = db.DatabaseId
    JOIN DBC.TVM   tv ON ou.ObjectId   = tv.TVMId
    WHERE ou.FieldId     IS NULL
      AND ou.IndexNumber IS NULL
) u ON s.DatabaseName = u.DatabaseName 
   AND s.TableName    = u.ObjectName
WHERE CAST(s.LastCollectTimeStamp AS DATE) >= CURRENT_DATE - 30
  AND s.DatabaseName <> 'DBC'
  AND s.StatsId <> 0
GROUP BY 1, 2
HAVING (Last_Actual_Access < CURRENT_DATE - 30 OR Last_Actual_Access IS NULL)
   AND (Last_Stat_Collect  > Last_Actual_Access OR Last_Actual_Access IS NULL)
ORDER BY Size_GB DESC;