-- =============================================================================
-- Component   : Skipped and Sample Statistics
-- =============================================================================
-- Description : Identifies stats being skipped or using sample
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

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
