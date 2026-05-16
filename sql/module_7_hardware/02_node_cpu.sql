-- =============================================================================
-- Component   : Node CPU Usage
-- =============================================================================
-- Description : Queries CPU usage by node for the current day
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    TheDate,
    NodeID,
    SUM(CPUIdle) AS TotalIdle,
    SUM(CPUUServ) AS TotalServ
FROM DBC.ResUsageSpma
WHERE TheDate = CURRENT_DATE
GROUP BY 1, 2
ORDER BY 1, 2;
