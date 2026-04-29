-- Module 7 Schema: NoPI Tables
-- Identifies tables without Primary Index
-- Cross-reference DBC.TablesV with DBC.IndicesV

LOCKING ROW FOR ACCESS
SELECT 
    t.DatabaseName,
    t.TableName,
    t.TableKind,
    t.ProtectionType,
    t.CreateTimeStamp
FROM DBC.TablesV t
WHERE t.TableKind = 'T'
  AND t.DatabaseName NOT IN ({{system_databases}})
  AND NOT EXISTS (
    SELECT 1 
    FROM DBC.IndicesV i
    WHERE i.DatabaseName = t.DatabaseName
      AND i.TableName = t.TableName
      AND i.IndexType IN ('P', 'Q')
  )
ORDER BY t.DatabaseName, t.TableName;
