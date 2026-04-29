-- Component 1: ResUsage Rules
-- Validates ResUsageSPMA and ResUsage collection intervals
-- Queries DBC.ResUsageRules for NodeLoggingRate, Active Filter Mode, and Summary Mode
-- Note: This view may not be available in all Teradata versions

LOCKING ROW FOR ACCESS
SELECT 
    RuleName,
    RuleValue,
    RuleType
FROM DBC.ResUsageRules
WHERE RuleName IN ('NodeLoggingRate', 'ActiveFilterMode', 'SummaryMode')
ORDER BY RuleName;
