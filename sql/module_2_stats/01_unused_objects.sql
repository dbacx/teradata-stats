-- Component 1: Unused Objects
-- Identifies statistics collections on tables with no recent usage
-- Concept: Tables with stats collection consuming CPU but no actual usage

SELECT 
    s.DatabaseName, 
    s.TableName,
    CAST(SUM(t.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    MAX(CAST(s.LastCollectTimeStamp AS DATE)) AS Last_Stat_Collect,
    MAX(u.LastAccessTimeStamp) AS Last_Actual_Access
FROM DBC.StatsV s
INNER JOIN (
    SELECT DatabaseName, TableName, TableKind, SUM(CurrentPerm) AS CurrentPerm
    FROM DBC.TableSizeV
    GROUP BY 1, 2, 3
) t ON s.DatabaseName = t.DatabaseName AND s.TableName = t.TableName
LEFT JOIN DBC.ObjectUsage u 
    ON s.DatabaseName = u.DatabaseName 
    AND s.TableName = u.TableName
WHERE t.TableKind = 'T'
  AND CAST(s.LastCollectTimeStamp AS DATE) >= CURRENT_DATE - 30
GROUP BY 1, 2
HAVING (Last_Actual_Access < CURRENT_DATE - 30 OR Last_Actual_Access IS NULL)
   AND Last_Stat_Collect > Last_Actual_Access
ORDER BY Size_GB DESC;
