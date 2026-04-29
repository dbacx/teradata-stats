-- Component 2: DBQL Rules
-- Lists all active DBQL logging rules
-- Queries DBC.DBQLRules for UserName, AccountString, Type, and logging options

LOCKING ROW FOR ACCESS
SELECT 
    UserName,
    AccountString,
    RuleType,
    LoggingOption,
    IsActive
FROM DBC.DBQLRules
WHERE IsActive = 'Y'
ORDER BY UserName, AccountString, RuleType;
