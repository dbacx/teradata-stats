-- Module 1 Health: Active Sessions
-- Count of sessions by user
-- Uses DBC.SessionInfo to identify users with high concurrent sessions

LOCKING ROW FOR ACCESS
SELECT 
    UserName,
    COUNT(*) as SessionCount
FROM DBC.SessionInfo
WHERE UserName IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;
