-- Component 5: Missing at Index Level
-- Identifies indexes (PI, SI, JI, PPI) without statistics
-- Concept: Missing stats on indexes prevents optimal index usage

SELECT DISTINCT 
    i.DatabaseName, 
    i.TableName,
    i.IndexType,
    i.IndexName
FROM DBC.IndicesV i
LEFT JOIN DBC.StatsV s ON i.DatabaseName = s.DatabaseName AND i.TableName = s.TableName AND i.ColumnName = s.ColumnName
WHERE i.IndexType IN ('P', 'S', 'Q', 'J') 
  AND s.ColumnName IS NULL
ORDER BY i.DatabaseName, i.TableName, i.IndexType;
