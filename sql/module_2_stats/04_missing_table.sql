-- Component 4: Missing at Table Level
-- Identifies actively used tables without any statistics
-- Concept: Tables used yesterday but without any stats collection

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
