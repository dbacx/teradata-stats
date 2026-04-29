-- Module 7 Schema: Fallback Tables
-- Identifies tables with Fallback protection enabled (duplicates space)
-- Uses DBC.TablesV with ProtectionType filtering

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
