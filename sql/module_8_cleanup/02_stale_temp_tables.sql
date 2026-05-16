-- =============================================================================
-- Component   : Stale Temp Tables
-- =============================================================================
-- Description : Identifies tables created more than 30 days ago that appear to be temporary or staging
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    DatabaseName,
    TableName,
    TableKind,
    CreateTimeStamp,
    LastAlterTimeStamp
FROM DBC.TablesV
WHERE TableKind = 'T'
  AND DatabaseName NOT IN ({{system_databases}})
  AND (UPPER(TableName) LIKE '%TEMP%' 
       OR UPPER(TableName) LIKE '%TMP%' 
       OR UPPER(TableName) LIKE '%STG%'
       OR UPPER(TableName) LIKE '%STAGE%'
       OR UPPER(TableName) LIKE '%WORK%')
  AND CreateTimeStamp < CURRENT_DATE - 30
ORDER BY CreateTimeStamp;
