-- =============================================================================
-- Component   : Large CHAR Columns
-- =============================================================================
-- Description : Identifies columns using fixed CHAR instead of VARCHAR for large sizes
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

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
