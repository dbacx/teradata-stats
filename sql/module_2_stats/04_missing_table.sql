-- =============================================================================
-- Component   : Missing at Table Level
-- =============================================================================
-- Description : Identifies actively used tables without any statistics
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT DISTINCT 
    o.ObjectDatabaseName AS DatabaseName, 
    o.ObjectTableName AS TableName,
    t.TableKind
FROM PDCRINFO.DBQLObjTbl_Hst o
INNER JOIN DBC.TablesV t ON o.ObjectDatabaseName = t.DatabaseName AND o.ObjectTableName = t.TableName
LEFT JOIN DBC.StatsV s ON o.ObjectDatabaseName = s.DatabaseName AND o.ObjectTableName = s.TableName
WHERE o.LogDate = CURRENT_DATE - 1 
  AND t.TableKind = 'T' 
  AND o.ObjectType = 'Tab' 
  AND s.TableName IS NULL
ORDER BY DatabaseName, TableName;
