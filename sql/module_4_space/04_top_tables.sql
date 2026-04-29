-- Component 4: Top Tables by Size
-- Ranking of the 50 heaviest tables in the system
-- Aggregates CurrentPerm from DBC.TableSizeV

LOCKING ROW FOR ACCESS
SELECT TOP 50
    DatabaseName,
    TableName,
    CAST(SUM(CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    CAST(SUM(PeakPerm) / (1024.0**3) AS DECIMAL(18,2)) AS PeakSize_GB,
    COUNT(*) AS AMP_Count
FROM DBC.TableSizeV
WHERE DatabaseName NOT IN ({system_databases})
GROUP BY DatabaseName, TableName
ORDER BY Size_GB DESC;
