-- =============================================================================
-- Component   : DBQL Rules
-- =============================================================================
-- Description : Lists all active DBQL logging rules
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    UserName,
    AccountString,
    RuleType,
    LoggingOption,
    IsActive
FROM DBC.DBQLRulesV
WHERE UPPER(IsActive) = 'Y'
ORDER BY UserName, AccountString, RuleType;
