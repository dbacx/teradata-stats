-- =============================================================================
-- Component 3.09: Top 20 Unused Databases By Size
-- =============================================================================
-- Description : Identifies large databases with zero aggregate usage in PDCR 
--               over the last 30 days. Prime candidates for cold storage.
-- Version     : 1.0.0
-- Author      : Ricardo Enciso
-- =============================================================================

WITH Size_DB AS (
    SELECT DatabaseName, SUM(CurrentPerm) AS CurrentPerm_Bytes
    FROM DBC.DiskSpaceV
    GROUP BY 1
    HAVING SUM(CurrentPerm) > 10737418240 -- > 10 GB
),
Uso_PDCR AS (
    SELECT ObjectDatabaseName, SUM(FreqofUse) AS Access_Count
    FROM PDCRINFO.DBQLObjTbl_Hst
    WHERE LogDate >= CURRENT_DATE - 30
    GROUP BY 1
)
SELECT TOP 20
    s.DatabaseName,
    CAST(s.CurrentPerm_Bytes / (1024.0**3) AS DECIMAL(18,2)) AS Total_Size_GB,
    COALESCE(u.Access_Count, 0) AS Total_Queries_30D
FROM Size_DB s
LEFT JOIN Uso_PDCR u
    ON s.DatabaseName = u.ObjectDatabaseName
WHERE COALESCE(u.Access_Count, 0) = 0
  AND s.DatabaseName NOT IN ('DBC', 'PDCRDATA', 'PDCRINFO', 'SYSADMIN')
ORDER BY Total_Size_GB DESC;