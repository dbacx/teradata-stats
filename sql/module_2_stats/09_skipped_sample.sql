-- Component 9: Skipped and Sample Statistics
-- Identifies stats being skipped or using sample
-- Concept: Monitor skipped stats and sample usage for accuracy concerns

SELECT DISTINCT 
    DatabaseName, 
    TableName,
    SampleSizePct,
    StatsSkipCount,
    SampleSignature
FROM DBC.StatsV 
WHERE (SampleSizePct > 0 AND SampleSizePct < 100) 
   OR StatsSkipCount > 0
ORDER BY DatabaseName, TableName;
