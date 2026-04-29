-- =============================================================================
-- Component   : Empty Tables
-- =============================================================================
-- Description : Identifies tables that are not consuming space (zero rows/blocks)
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
    t.CreateTimeStamp
FROM DBC.TablesV t
WHERE t.TableKind = 'T'
  AND t.DatabaseName NOT IN ({{system_databases}})
  AND NOT EXISTS (
    SELECT 1 
    FROM DBC.TableSizeV s
    WHERE s.DatabaseName = t.DatabaseName
      AND s.TableName = t.TableName
      AND s.CurrentPerm > 0
  )
ORDER BY t.DatabaseName, t.TableName;
