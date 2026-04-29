-- =============================================================================
-- Component   : Skewed Tables
-- =============================================================================
-- Description : Identifies tables with data skew across AMPs
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

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
