-- =============================================================================
-- Component   : Sqltextinfo068
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================
 LOCK ROW

   FOR ACCESS
SELECT DATE (FORMAT 'yyyy-mm-dd') (NAMED logdate) , infodata (NAMED db_version)

  FROM dbc.dbcinfo

 WHERE infokey = 'VERSION' ;