-- Component 3: Missing at PARTITION level
-- Identifies PPI tables without PARTITION column statistics
-- Concept: Partition Elimination requires PARTITION stats

SELECT DISTINCT 
    ic.DatabaseName, 
    ic.TableName,
    t.PartitioningLevels
FROM DBC.IndexConstraints ic
INNER JOIN DBC.TablesV t ON ic.DatabaseName = t.DatabaseName AND ic.TableName = t.TableName
LEFT JOIN DBC.StatsV s ON ic.DatabaseName = s.DatabaseName AND ic.TableName = s.TableName AND s.ColumnName = 'PARTITION'
WHERE ic.ConstraintType = 'Q' 
  AND s.ColumnName IS NULL
  AND t.PartitioningLevels > 0
ORDER BY ic.DatabaseName, ic.TableName;
