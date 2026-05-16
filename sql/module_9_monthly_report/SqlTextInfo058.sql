-- =============================================================================
-- Component   : Sqltextinfo058
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
SELECT logdate,
       COUNT (TABLENAME)

  FROM pdcrinfo.tablespace_hst

 WHERE logdate >= DATE - 90

 GROUP BY 1 ;