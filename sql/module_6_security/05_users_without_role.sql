-- Component 5: Users Without Role
-- Identifies productive users that are not members of any role
-- Queries DBC.UsersV left joined with DBC.RoleMembersV

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
