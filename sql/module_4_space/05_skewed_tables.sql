-- Component 5: Skewed Tables
-- Identifies tables with data skew across AMPs
-- Calculates skew percentage: (100 - (AVG(CurrentPerm)/MAX(CurrentPerm))*100)

LOCKING ROW FOR ACCESS
SELECT 
    DatabaseName,
    TableName,
    SUM(CurrentPerm) AS Total_CurrentPerm_Bytes,
    MAX(CurrentPerm) AS Max_CurrentPerm_Bytes,
    AVG(CurrentPerm) AS Avg_CurrentPerm_Bytes,
    MIN(CurrentPerm) AS Min_CurrentPerm_Bytes,
    COUNT(*) AS AMP_Count,
    CAST((100 - (AVG(CurrentPerm) * 100.0 / NULLIF(MAX(CurrentPerm), 0))) AS DECIMAL(18,2)) AS Skew_Pct
FROM DBC.TableSizeV
WHERE DatabaseName NOT IN ({system_databases})
GROUP BY DatabaseName, TableName
HAVING (100 - (AVG(CurrentPerm) * 100.0 / NULLIF(MAX(CurrentPerm), 0))) > {skew_pct_threshold}
ORDER BY Skew_Pct DESC;
