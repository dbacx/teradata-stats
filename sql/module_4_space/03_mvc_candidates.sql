-- =============================================================================
-- Component   : MVC (Multi-Value Compression) Candidates
-- =============================================================================
-- Description : Identifies compressible columns without compression in large tables
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
    CAST(SUM(ts.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS TableSize_GB,
    c.CompressValueList
FROM DBC.ColumnsV c
INNER JOIN DBC.TableSizeV ts ON c.DatabaseName = ts.DatabaseName AND c.TableName = ts.TableName
WHERE c.DatabaseName NOT IN ({system_databases})
  AND c.ColumnType IN ('CF', 'CV', 'I1', 'I2', 'I4', 'I8', 'D', 'DA')  -- CHAR, VARCHAR, BYTEINT, SMALLINT, INTEGER, BIGINT, DATE
  AND c.CompressValueList IS NULL
GROUP BY c.DatabaseName, c.TableName, c.ColumnName, c.ColumnType, c.ColumnLength, c.CompressValueList
HAVING SUM(ts.CurrentPerm) / (1024.0**3) > 10  -- Tables larger than 10GB
ORDER BY TableSize_GB DESC;
