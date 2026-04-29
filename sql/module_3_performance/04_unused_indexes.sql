-- Component 4: Unused/Review Indexes
-- Lists Secondary Indexes on large tables (>10GB) for review
-- Crosses DBC.IndicesV with DBC.TableSizeV

LOCKING ROW FOR ACCESS
SELECT 
    i.DatabaseName,
    i.TableName,
    i.IndexName,
    i.IndexType,
    i.IndexNumber,
    i.UniqueFlag,
    CAST(SUM(ts.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS TableSize_GB,
    i.ColumnNames
FROM DBC.IndicesV i
INNER JOIN DBC.TableSizeV ts ON i.DatabaseName = ts.DatabaseName AND i.TableName = ts.TableName
WHERE i.DatabaseName NOT IN {system_databases}
  AND i.IndexType IN ('SI', 'NUSI', 'USI')
  AND CAST(SUM(ts.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) > 10
GROUP BY i.DatabaseName, i.TableName, i.IndexName, i.IndexType, i.IndexNumber, i.UniqueFlag, i.ColumnNames
ORDER BY TableSize_GB DESC, i.DatabaseName, i.TableName;
