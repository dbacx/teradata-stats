-- =============================================================================
-- Component   : Multicolumn MaxValueLength
-- =============================================================================
-- Description : Identifies multicolumn stats where combined length exceeds storage limits
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT DISTINCT 
    DatabaseName, 
    TableName,
    ColumnName,
    ExpressionCount,
    MaxValueLength
FROM DBC.StatsV 
WHERE ExpressionCount > 1
  AND MaxValueLength < {max_value_length_threshold}
ORDER BY DatabaseName, TableName;
