-- Module 9 Cleanup: Stale Temp Tables
-- Identifies tables created more than 30 days ago that appear to be temporary or staging
-- Uses pattern matching on table names to identify temp/staging tables
-- Excludes system databases

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
