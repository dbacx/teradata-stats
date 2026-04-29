-- =============================================================================
-- Component   : Spool Usage Alerts
-- =============================================================================
-- Description : Identifies large tables that may cause spool issues
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    t.DatabaseName,
    t.TableName,
    t.TableKind,
    CAST(SUM(ts.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    CAST(SUM(ts.PeakPerm) / (1024.0**3) AS DECIMAL(18,2)) AS PeakSize_GB,
    'REVIEW_SPOOL' AS Recommendation
FROM DBC.TablesV t
LEFT JOIN DBC.TableSizeV ts ON t.DatabaseName = ts.DatabaseName AND t.TableName = ts.TableName
WHERE t.DatabaseName NOT IN {system_databases}
  AND t.TableKind = 'T'
  AND CAST(SUM(ts.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) > 10
GROUP BY t.DatabaseName, t.TableName, t.TableKind
ORDER BY Size_GB DESC;
