-- Component 1: Full Table Scans
-- Identifies tables with high I/O usage (potential FTS) in the last 30 days
-- Queries DBC.TablesV with size metrics as fallback for PDCRINFO

LOCKING ROW FOR ACCESS
SELECT 
    t.DatabaseName,
    t.TableName,
    t.TableKind,
    CAST(SUM(ts.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    CAST(SUM(ts.PeakPerm) / (1024.0**3) AS DECIMAL(18,2)) AS PeakSize_GB
FROM DBC.TablesV t
LEFT JOIN DBC.TableSizeV ts ON t.DatabaseName = ts.DatabaseName AND t.TableName = ts.TableName
WHERE t.DatabaseName NOT IN {system_databases}
  AND t.TableKind = 'T'
  AND CAST(SUM(ts.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) > 1
GROUP BY t.DatabaseName, t.TableName, t.TableKind
ORDER BY Size_GB DESC;
