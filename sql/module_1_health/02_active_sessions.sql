-- =============================================================================
-- Component   : Active Sessions
-- =============================================================================
-- Description : Count of sessions by user from DBC.SessionInfo
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    UserName,
    COUNT(*) as SessionCount
FROM DBC.SessionInfo
WHERE UserName IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;
