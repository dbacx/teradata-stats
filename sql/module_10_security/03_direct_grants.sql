-- =============================================================================
-- Component   : Direct Grants
-- =============================================================================
-- Description : Identifies users with direct grants instead of role-based access
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    UserName,
    DatabaseName,
    TableName,
    ColumnName,
    AccessRight,
    GrantorName,
    GrantTimeStamp
FROM DBC.AllRightsV
WHERE UserName <> RoleName
  AND DatabaseName NOT IN ({system_databases})
ORDER BY UserName, DatabaseName, TableName;
