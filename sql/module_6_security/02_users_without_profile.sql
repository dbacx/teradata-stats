-- =============================================================================
-- Component   : Users Without Profile
-- =============================================================================
-- Description : Identifies users without an assigned profile
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
    DefaultDatabase,
    ProfileName
FROM DBC.UsersV
WHERE ProfileName IS NULL
ORDER BY UserName;
