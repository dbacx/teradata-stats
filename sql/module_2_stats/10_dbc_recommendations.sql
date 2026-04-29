-- =============================================================================
-- Component   : DBC Recommendations
-- =============================================================================
-- Description : Identifies missing stats in system databases (DBC, PDCRDATA)
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT DISTINCT 
    t.DatabaseName, 
    t.TableName,
    t.TableKind
FROM DBC.TablesV t
LEFT JOIN DBC.StatsV s ON t.DatabaseName = s.DatabaseName AND t.TableName = s.TableName
WHERE t.DatabaseName IN ('DBC', 'PDCRDATA', 'PDCRINFO') 
  AND t.TableKind = 'T' 
  AND s.TableName IS NULL
ORDER BY t.DatabaseName, t.TableName;
