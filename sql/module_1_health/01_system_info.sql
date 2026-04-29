-- =============================================================================
-- Component   : System Information
-- =============================================================================
-- Description : Basic query for version and release information from DBC.DBCInfo
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    InfoKey,
    InfoData
FROM DBC.DBCInfo
ORDER BY InfoKey;
