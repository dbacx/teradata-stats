-- =============================================================================
-- Component   : Sqltextinfo065
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
SELECT logdate(FORMAT 'yyyy-mm-dd') (NAMED logdate) , AVG (totaliocount) (NAMED io_average)

  FROM pdcrinfo.dbqlogtbl_hst

 WHERE logdate >= DATE - 90

 GROUP BY logdate

 ORDER BY logdate ;