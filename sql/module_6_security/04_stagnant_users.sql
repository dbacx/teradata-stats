-- Component 4: Stagnant Users
-- Identifies users with no recent access
-- Queries DBC.UsersV for users with stale LastAccessTimeStamp

LOCKING ROW FOR ACCESS
SELECT 
    UserName,
    CreatorName,
    LastAccessTimeStamp,
    CAST(CURRENT_DATE - LastAccessTimeStamp AS INTEGER) AS DaysSinceLastAccess
FROM DBC.UsersV
WHERE LastAccessTimeStamp < CURRENT_DATE - {unused_days_threshold}
ORDER BY LastAccessTimeStamp ASC;
