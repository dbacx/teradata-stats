-- =============================================================================
-- Component   : Unused Tables Space
-- =============================================================================
-- Description : Identifies tables with no recent access and their space consumption
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    ts.DatabaseName,
    ts.TableName,
    t.TableKind,
    CAST(SUM(ts.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    CAST(SUM(ts.PeakPerm) / (1024.0**3) AS DECIMAL(18,2)) AS PeakSize_GB,
    MAX(ou.LastAccessTimeStamp) AS LastAccessTimeStamp,
    t.CreateTimeStamp
FROM DBC.TableSizeV ts
INNER JOIN DBC.TablesV t ON ts.DatabaseName = t.DatabaseName AND ts.TableName = t.TableName
LEFT JOIN DBC.ObjectUsage ou 
    ON ts.DatabaseName = ou.DatabaseName 
    AND ts.TableName = ou.TableName
    AND (ou.LastAccessTimeStamp IS NULL 
         OR ou.LastAccessTimeStamp < CURRENT_DATE - {unused_days_threshold})
WHERE ts.DatabaseName NOT IN ({system_databases})
  AND t.TableKind = 'T'
GROUP BY ts.DatabaseName, ts.TableName, t.TableKind, t.CreateTimeStamp
HAVING SUM(ts.CurrentPerm) > 0
ORDER BY Size_GB DESC;
