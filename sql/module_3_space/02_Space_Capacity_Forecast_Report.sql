-- =============================================================================
-- Component 3.02: Space Capacity Forecast Report
-- =============================================================================
-- Description : Forecasts database capacity exhaustion using 30-day historical 
--               growth trends from PDCRINFO.DatabaseSpace_Hst.
-- Version     : 1.0.0
-- Author      : Ricardo Enciso
-- =============================================================================

WITH Historia_30D AS (
    SELECT 
        DatabaseName,
        MIN(LogDate) AS Start_Date,
        MAX(LogDate) AS End_Date,
        SUM(CASE WHEN LogDate = CURRENT_DATE - 30 THEN CurrentPerm ELSE 0 END) AS Start_Perm,
        SUM(CASE WHEN LogDate = CURRENT_DATE - 1 THEN CurrentPerm ELSE 0 END) AS End_Perm,
        MAX(MaxPerm) AS Current_MaxPerm
    FROM PDCRINFO.DatabaseSpace_Hst
    WHERE LogDate IN (CURRENT_DATE - 30, CURRENT_DATE - 1)
    GROUP BY 1
    HAVING Current_MaxPerm > 0
)
SELECT 
    DatabaseName,
    Start_Perm / (1024.0**3) AS Size_30_Days_Ago_GB,
    End_Perm / (1024.0**3) AS Current_Size_GB,
    Current_MaxPerm / (1024.0**3) AS MaxPerm_GB,
    CAST((End_Perm - Start_Perm) / (1024.0**3) AS DECIMAL(18,2)) AS Growth_30D_GB,
    -- Crecimiento diario promedio
    CAST(((End_Perm - Start_Perm) / 29.0) / (1024.0**3) AS DECIMAL(18,2)) AS Avg_Daily_Growth_GB,
    -- Proyección lineal a 95% de ocupación
    CASE 
        WHEN (End_Perm - Start_Perm) <= 0 THEN 9999 -- No crece o decreció
        ELSE CAST(((Current_MaxPerm * 0.95) - End_Perm) / NULLIF(((End_Perm - Start_Perm) / 29.0), 0) AS INTEGER) 
    END AS Days_Until_95_Percent
FROM Historia_30D
WHERE DatabaseName NOT IN ('DBC', 'PDCRDATA', 'PDCRINFO', 'SYSDBA')
ORDER BY Days_Until_95_Percent ASC;