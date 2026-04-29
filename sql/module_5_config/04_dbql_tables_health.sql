-- Component 4: DBQL Tables Health
-- Checks the health and size of DBQL logging tables
-- Queries DBC.TablesV and DBC.TableSizeV for DBQLog tables

LOCKING ROW FOR ACCESS
SELECT 
    DatabaseName,
    TableName AS ViewName,
    CommentString
FROM DBC.TablesV 
WHERE UPPER(DatabaseName) = 'PDCRINFO' 
  AND UPPER(TableKind) = 'V' 
  AND UPPER(TableName) LIKE 'DBQL%HST'
ORDER BY TableName;
