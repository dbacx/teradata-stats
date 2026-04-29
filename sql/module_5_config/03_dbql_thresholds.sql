-- =============================================================================
-- Component   : DBQL Thresholds
-- =============================================================================
-- Description : Checks if thresholds are configured for DBQL rules
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
    SQLTextTime,
    SQLTextIO,
    SummaryRate
FROM DBC.DBQLRulesV
WHERE UPPER(IsActive) = 'Y'
  AND (SQLTextTime IS NOT NULL OR SQLTextIO IS NOT NULL)
ORDER BY UserName, AccountString;
