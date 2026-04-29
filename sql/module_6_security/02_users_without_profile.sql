-- Component 2: Users Without Profile
-- Identifies users without an assigned profile
-- Queries DBC.UsersV for users with NULL ProfileName

LOCKING ROW FOR ACCESS
SELECT 
    UserName,
    CreatorName,
    DefaultDatabase,
    ProfileName
FROM DBC.UsersV
WHERE ProfileName IS NULL
ORDER BY UserName;
