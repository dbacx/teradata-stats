-- =============================================================================
-- Component   : Sqltextinfo015
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================
 LOCK ROW ACCESS
SELECT CURRENTPERMRnk,
       year_of_calendar,
       Month_of_Year,
       Week_of_year,
       LogDate,
       DatabaseName,

       AccountName,
       CURRENTPERM,
       CURRENTPERM / (1024 * 1024 * 1024) AS CURRENTPERM_Gb,

       PEAKPERM,
       MAXPERM,
       MAXPERM / (1024 * 1024 * 1024) AS MAXPERM_Gb,
       CURRENTPERMSKEW,

       PERMPCTUSED

  FROM

        (SELECT RANK () OVER (
                          ORDER BY CURRENTPERM DESC) AS CURRENTPERMRnk,
               c.year_of_calendar,
               c.Month_of_Year,
               c.Week_of_year,
               LogDate,
               DatabaseName,

               AccountName,
               CURRENTPERM,
               CURRENTPERM / (1024 * 1024 * 1024) AS CURRENTPERM_Gb,

               PEAKPERM,
               MAXPERM,
               MAXPERM / (1024 * 1024 * 1024) AS MAXPERM_Gb,
               CURRENTPERMSKEW,

               PERMPCTUSED

          FROM PDCRINFO.DatabaseSpace_Hst a

         INNER JOIN PDCRINFO.CALENDAR c
            ON a.Logdate = c.Calendar_date

         WHERE c.Calendar_date = a.Logdate

           AND a.Logdate =

                (SELECT MAXIMUM (Logdate)

                  FROM PDCRINFO.DatabaseSpace_Hst
               )
       ) dt

 WHERE CURRENTPERMRnk <= 10 ;