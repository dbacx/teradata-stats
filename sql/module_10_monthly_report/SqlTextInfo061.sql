-- =============================================================================
-- Component   : Sqltextinfo061
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
SELECT logdate(FORMAT 'yyyy-mm-dd'),
       COUNT (logdate) (NAMED query_complexity)

  FROM

        (SELECT logdate,
               queryid,
               COUNT (stepname) AS step

          FROM pdcrinfo.dbqlsteptbl_hst

         WHERE logdate >= DATE - 90

           AND stepname = 'JIN'

         GROUP BY logdate,
                  queryid

        HAVING step > 5
       ) results

 GROUP BY logdate ;