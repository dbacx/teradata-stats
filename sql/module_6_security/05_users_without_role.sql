-- =============================================================================
-- Component   : Users Without Role
-- =============================================================================
-- Description : Identifies productive users that are not members of any role
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    u.UserName,
    u.CreatorName,
    u.DefaultDatabase,
    u.LastAccessTimeStamp
FROM DBC.UsersV u
LEFT JOIN DBC.RoleMembersV r ON u.UserName = r.RoleName
WHERE r.RoleName IS NULL
  AND u.UserName NOT IN ('DBC', 'SysAdmin', 'TDWM')
ORDER BY u.UserName;
