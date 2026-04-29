-- Component 8: Multicolumn MaxValueLength
-- Identifies multicolumn stats where combined length exceeds storage limits
-- Concept: Multicolumn stats can be truncated if MaxValueLength is too small

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
