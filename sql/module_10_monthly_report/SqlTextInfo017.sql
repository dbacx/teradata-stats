-- =============================================================================
-- Component   : Sqltextinfo017
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================
 LOCK ROW ACCESS
SELECT SpoolUtilizedRnk,
       UserName,
       Total_CURRENTSPOOL

  FROM

        (SELECT dense_Rank() OVER (
                               ORDER BY SUM (CURRENTSPOOL) DESC) AS SpoolUtilizedRnk,
               UserName,
               SUM (CURRENTSPOOL) (NAMED Total_CURRENTSPOOL)

          FROM PDCRINFO.SpoolSpace_hst a

         INNER JOIN PDCRINFO.CALENDAR c
            ON a.Logdate = c.Calendar_date

         WHERE c.Calendar_date = a.Logdate

           AND a.logdate BETWEEN dAtE'2026-04-01' AND dAtE'2026-04-30'

         GROUP BY 2
       ) dt

 WHERE SpoolUtilizedRnk <= 10 ;