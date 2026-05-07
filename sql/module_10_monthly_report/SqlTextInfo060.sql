-- =============================================================================
-- Component   : Sqltextinfo060
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
SELECT logdate(FORMAT 'yyyy-mm-dd') , (CAST(CAST(COUNT (queryid) AS FLOAT) / 24 AS FLOAT)) (NAMED queryAverage)

  FROM pdcrinfo.DBQLogTbl_hst

 WHERE logdate >= DATE - 90

 GROUP BY logdate

 ORDER BY logdate ;