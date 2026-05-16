-- =============================================================================
-- Component   : Fallback Tables
-- =============================================================================
-- Description : Identifies tables with Fallback protection enabled (duplicates space)
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
    t.ProtectionType,
    t.TableKind,
    t.CreateTimeStamp,
    t.LastAlterTimeStamp
FROM DBC.TablesV t
WHERE t.ProtectionType = 'F'
  AND t.TableKind = 'T'
  AND t.DatabaseName NOT IN ({{system_databases}})
ORDER BY t.DatabaseName, t.TableName;
