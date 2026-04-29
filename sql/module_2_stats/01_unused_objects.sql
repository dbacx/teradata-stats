-- =============================================================================
-- Component 1: Unused Objects - Statistics Collection Without Table Usage
-- =============================================================================
-- Description : Identifies tables with recent statistics collection (last 30 days)
--               but no actual usage, suggesting wasted CPU on COLLECT STATS
-- 
-- Version     : 1.0.0
-- Date        : 2025-04-29
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
    -- Table size in GB (sum across AMPs)
    CAST(SUM(t.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    -- Most recent COLLECT STATS execution on this table
    MAX(CAST(s.LastCollectTimeStamp AS DATE))                AS Last_Stat_Collect,
    -- Most recent actual access recorded by DBQL ObjectUsage
    MAX(u.LastAccessTimeStamp)                               AS Last_Actual_Access

FROM DBC.StatsV s   -- Statistics metadata (excludes SUMMARY via StatsId <> 0)

-- Table size: join TableSizeV (size per AMP) with TablesV (to get TableKind)
-- TableKind not available in TableSizeV, requires join to TablesV
INNER JOIN (
    SELECT ts.DatabaseName, ts.TableName, SUM(ts.CurrentPerm) AS CurrentPerm
    FROM DBC.TableSizeV ts
    JOIN DBC.TablesV tb 
        ON ts.DatabaseName = tb.DatabaseName 
        AND ts.TableName   = tb.TableName
    WHERE tb.TableKind = 'T'   -- Permanent tables only
    GROUP BY 1, 2
) t ON s.DatabaseName = t.DatabaseName 
   AND s.TableName    = t.TableName

-- Object usage: DBC.ObjectUsage stores IDs (not names), requires resolution
-- via DBC.Dbase and DBC.TVM. LastAccessTimeStamp not exposed in any DBC view.
-- FieldId IS NULL + IndexNumber IS NULL = table-level access (not column/index)
LEFT JOIN (
    SELECT 
        db.DatabaseName,
        tv.TVMName          AS ObjectName,
        ou.LastAccessTimeStamp
    FROM DBC.ObjectUsage ou
    JOIN DBC.Dbase db ON ou.DatabaseId = db.DatabaseId
    JOIN DBC.TVM   tv ON ou.ObjectId   = tv.TVMId
    WHERE ou.FieldId     IS NULL   -- Exclude column/index-level entries
      AND ou.IndexNumber IS NULL   -- Table-level access only
) u ON s.DatabaseName = u.DatabaseName 
   AND s.TableName    = u.ObjectName

WHERE CAST(s.LastCollectTimeStamp AS DATE) >= CURRENT_DATE - 30  -- Stats collected in last 30 days
  AND s.DatabaseName <> 'DBC'   -- DBC objects not monitored by ObjectUsage (per Teradata doc)
  AND s.StatsId <> 0            -- Exclude SUMMARY statistics (COLLECT SUMMARY STATS)

GROUP BY 1, 2

-- Keep only tables where:
-- 1. No access in last 30 days (or never accessed)
-- 2. Stats were collected after the last actual access (wasted collection)
-- NULL-safe: if Last_Actual_Access IS NULL, both conditions evaluate correctly
HAVING (Last_Actual_Access < CURRENT_DATE - 30 OR Last_Actual_Access IS NULL)
   AND (Last_Stat_Collect  > Last_Actual_Access OR Last_Actual_Access IS NULL)

ORDER BY Size_GB DESC;  -- Prioritize largest tables (highest potential CPU savings)