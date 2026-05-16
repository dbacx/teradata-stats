-- =============================================================================
-- Component   : Stagnant Users
-- =============================================================================
-- Description : Identifies users with no recent access
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    UserName,
    CreatorName,
    LastAccessTimeStamp,
    CAST(CURRENT_DATE - LastAccessTimeStamp AS INTEGER) AS DaysSinceLastAccess
FROM DBC.UsersV
WHERE LastAccessTimeStamp < CURRENT_DATE - {unused_days_threshold}
ORDER BY LastAccessTimeStamp ASC;
