-- Component 6: Stale Statistics
-- Identifies statistics with old collection timestamps
-- Concept: Stats older than threshold don't reflect current data reality

SELECT DISTINCT 
    DatabaseName, 
    TableName,
    MAX(CAST(LastCollectTimeStamp AS DATE)) AS Last_Collect_Date
FROM DBC.StatsV
WHERE CAST(LastCollectTimeStamp AS DATE) < CURRENT_DATE - {stale_days_threshold}
GROUP BY 1, 2
ORDER BY Last_Collect_Date ASC;
