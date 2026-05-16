-- =============================================================================
-- Component   : ResUsage Rules
-- =============================================================================
-- Description : Validates ResUsageSPMA and ResUsage collection intervals
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    RuleName,
    RuleValue,
    RuleType
FROM DBC.ResUsageRules
WHERE RuleName IN ('NodeLoggingRate', 'ActiveFilterMode', 'SummaryMode')
ORDER BY RuleName;
