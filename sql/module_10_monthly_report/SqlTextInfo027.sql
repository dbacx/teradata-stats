-- =============================================================================
-- Component   : Sqltextinfo027
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT CASE Month_of_Year
            WHEN 1        THEN 'January'
            WHEN 2        THEN 'February'
            WHEN 3        THEN 'March'
            WHEN 4        THEN 'April'
            WHEN 5        THEN 'May'
            WHEN 6        THEN 'June'
            WHEN 7        THEN 'July'
            WHEN 8        THEN 'August'
            WHEN 9        THEN 'September'
            WHEN 10       THEN 'October'
            WHEN 11       THEN 'November'
            WHEN 12       THEN 'December'

             END (NAMED Month_of_Year) , MAXIMUM (delaytime) AS DelayTime,

       CASE
                                       WHEN MAXIMUM (delaytime) < 60               THEN 'Healthy'

            WHEN MAXIMUM (delaytime) BETWEEN 60 AND 600 THEN 'Degraded'
            ELSE 'Critical'

             END AS Delay,
       'Delay Time' AS Delay_Time

  FROM pdcrinfo.dbqlogtbl_hst a

 INNER JOIN PDCRINFO.CALENDAR c
    ON a.logdate = c.calendar_date

 WHERE a.LogDate BETWEEN dAtE'2026-03-01' AND dAtE'2026-04-30'

 GROUP BY Month_of_Year

 ORDER BY Month_of_Year ;