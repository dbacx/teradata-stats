-- Component 4: DBQL Tables Health
-- Checks the health and size of DBQL logging tables
-- Queries DBC.TablesV and DBC.TableSizeV for DBQLog tables

LOCKING ROW FOR ACCESS
SELECT 
    t.DatabaseName,
    t.TableName,
    t.TableKind,
    t.CreateTimeStamp,
    t.LastAlterTimeStamp,
    CAST(SUM(ts.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    CAST(SUM(ts.PeakPerm) / (1024.0**3) AS DECIMAL(18,2)) AS PeakSize_GB
FROM DBC.TablesV t
LEFT JOIN DBC.TableSizeV ts ON t.DatabaseName = ts.DatabaseName AND t.TableName = ts.TableName
WHERE t.DatabaseName = 'DBC'
  AND t.TableName LIKE 'DBQLog%'
GROUP BY t.DatabaseName, t.TableName, t.TableKind, t.CreateTimeStamp, t.LastAlterTimeStamp
ORDER BY t.TableName;
