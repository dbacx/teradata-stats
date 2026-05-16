-- =============================================================================
-- Component   : Database Space Utilization
-- =============================================================================
-- Description : Queries DBC.DiskSpaceV to calculate space usage per database
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    DatabaseName,
    SUM(CurrentPerm) AS CurrentPerm_Bytes,
    SUM(MaxPerm) AS MaxPerm_Bytes,
    CAST(SUM(CurrentPerm) / (1024.0**4) AS DECIMAL(18,2)) AS CurrentPerm_TB,
    CAST(SUM(MaxPerm) / (1024.0**4) AS DECIMAL(18,2)) AS MaxPerm_TB,
    CAST((SUM(CurrentPerm) * 100.0 / NULLIF(SUM(MaxPerm), 0)) AS DECIMAL(18,2)) AS Usage_Pct
FROM DBC.DiskSpaceV
WHERE DatabaseName NOT IN ({system_databases})
GROUP BY DatabaseName
HAVING SUM(MaxPerm) > 0
ORDER BY Usage_Pct DESC;
