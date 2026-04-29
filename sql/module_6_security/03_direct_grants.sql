-- Component 3: Direct Grants
-- Identifies users with direct grants instead of role-based access
-- Queries DBC.AllRightsV for users with direct access rights

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
