-- Component 7: Zero Statistics
-- Identifies stats with RowCount=0 on tables that actually contain data
-- Concept: Zero count stats can cause Product Join plans on populated tables

SELECT 
    s.DatabaseName, 
    s.TableName,
    s.RowCount AS Stats_RowCount,
    CAST(SUM(ts.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Actual_Size_GB
FROM DBC.StatsV s 
INNER JOIN DBC.TableSizeV ts ON s.DatabaseName = ts.DatabaseName AND s.TableName = ts.TableName
WHERE s.RowCount = 0 
GROUP BY 1, 2, 3
HAVING SUM(ts.CurrentPerm) > 0
ORDER BY Actual_Size_GB DESC;
