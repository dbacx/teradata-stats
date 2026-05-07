-- =============================================================================
-- Component   : Sqltextinfo057
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
       COUNT (databasename)

  FROM pdcrinfo.DatabaseSpace_Hst

 WHERE logdate >= DATE - 90

 GROUP BY 1 ;