-- =============================================================================
-- Component   : Missing at Index Level
-- =============================================================================
-- Description : Identifies indexes (PI, SI, JI, PPI) without statistics
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

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
