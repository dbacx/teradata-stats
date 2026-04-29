-- =============================================================================
-- Component   : Stale Statistics
-- =============================================================================
-- Description : Identifies statistics with old collection timestamps
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT DISTINCT 
    DatabaseName, 
    TableName,
    MAX(CAST(LastCollectTimeStamp AS DATE)) AS Last_Collect_Date
FROM DBC.StatsV
WHERE CAST(LastCollectTimeStamp AS DATE) < CURRENT_DATE - {stale_days_threshold}
GROUP BY 1, 2
ORDER BY Last_Collect_Date ASC;
