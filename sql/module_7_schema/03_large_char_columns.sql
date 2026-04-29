-- Module 7 Schema: Large CHAR Columns
-- Identifies columns using fixed CHAR instead of VARCHAR for large sizes
-- Uses DBC.ColumnsV filtering for Char Fixed type with length > 100

LOCKING ROW FOR ACCESS
SELECT 
    c.DatabaseName,
    c.TableName,
    c.ColumnName,
    c.ColumnType,
    c.ColumnLength,
    c.Nullable,
    c.DefaultValue
FROM DBC.ColumnsV c
WHERE c.ColumnType = 'CF'
  AND c.ColumnLength > 100
  AND c.DatabaseName NOT IN ({{system_databases}})
ORDER BY c.DatabaseName, c.TableName, c.ColumnName;
