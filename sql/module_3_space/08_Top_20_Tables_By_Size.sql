-- =============================================================================
-- Component 3.08: Top 20 Tables By Size
-- =============================================================================
-- Description : Identifies the 20 largest physical tables in the cluster.
--               Includes Skew calculation to detect bad Primary Index choices.
-- Version     : 1.0.0
-- Author      : Ricardo Enciso
-- =============================================================================

SELECT TOP 20
    DatabaseName,
    TableName,
    CAST(SUM(CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Total_Size_GB,
    CAST(((MAX(CurrentPerm) - AVG(CurrentPerm)) / NULLIF(AVG(CurrentPerm), 0)) * 100 AS DECIMAL(5,2)) AS Skew_Pct
FROM DBC.TableSizeV
WHERE DatabaseName NOT IN ('DBC', 'PDCRDATA', 'SYSDBA')
GROUP BY 1, 2
ORDER BY Total_Size_GB DESC;