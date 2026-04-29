-- Component 3: DBQL Thresholds
-- Checks if thresholds are configured for DBQL rules
-- Queries DBC.DBQLRules for SQLTextTime and SQLTextIO thresholds

LOCKING ROW FOR ACCESS
SELECT 
    UserName,
    AccountString,
    RuleType,
    SQLTextTime,
    SQLTextIO,
    SummaryRate
FROM DBC.DBQLRulesV
WHERE UPPER(IsActive) = 'Y'
  AND (SQLTextTime IS NOT NULL OR SQLTextIO IS NOT NULL)
ORDER BY UserName, AccountString;
