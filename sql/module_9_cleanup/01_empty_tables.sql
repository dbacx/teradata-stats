-- Module 9 Cleanup: Empty Tables
-- Identifies tables that are not consuming space (zero rows/blocks)
-- Cross-references DBC.TablesV with DBC.TableSizeV to detect empty tables
-- Excludes system databases

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
