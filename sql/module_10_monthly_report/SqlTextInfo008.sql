-- =============================================================================
-- Component   : Sqltextinfo008
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
SELECT TheDate AS LogDate,
       sh.WorkPeriod,
       sh.PERIOD,

       CASE WHEN day_of_week = 1 THEN 'Sunday'
            WHEN day_of_week = 2 THEN 'Monday'
            WHEN day_of_week = 3 THEN 'Tuesday'
            WHEN day_of_week = 4 THEN 'Wednesday'
            WHEN day_of_week = 5 THEN 'Thursday'
            WHEN day_of_week = 6 THEN 'Friday'
            WHEN day_of_week = 7 THEN 'Saturday'

             END AS DayOfWeek,

       CASE WHEN day_of_week IN (1, 7)                                                                                      THEN 'Weekend'
            WHEN day_of_week IN (2, 3, 4,
                                                                           5, 6) THEN 'Weekday'
            ELSE 'Unknown'

             END AS WeekPeriod,
       SUM (FlowCtlCnt) AS FlowCtlCnt,
       SUM (CASE
                                                                   WHEN flowctlcnt > 0 THEN 1 ELSE 0
                                                               END) AS AMPS_In_FlwCntrl,
       SUM (CASE
                                                                                                  WHEN Available = 0 THEN 1
                                                                                                  ELSE 0
                                                                                              END) AS Zero_EofI_AWT_Count,

       SUM (CASE
                                                                                                      WHEN AvailableMin = 0 THEN 1
                                                                                                      ELSE 0
                                                                                                  END) AS Zero_AWT_Count,

       SUM (CASE
                                                                                                          WHEN AvailableMin BETWEEN 1 AND 5 THEN 1
                                                                                                          ELSE 0
                                                                                                      END) AS OneTo5_AWT_Count,

       SUM (CASE
                                                                                                              WHEN AvailableMin BETWEEN 5 AND 10 THEN 1
                                                                                                              ELSE 0
                                                                                                          END) AS FiveTo10_AWT_Count,

       SUM (CASE
                                                                                                                  WHEN AvailableMin > 10 THEN 1
                                                                                                                  ELSE 0
                                                                                                              END) AS GT10_AWT_Count,

       MAXIMUM (InuseMax) (NAMED Max_of_InUseMax) , MAXIMUM (FlowCtlTime / 1000) AS Max_FlowCtlTime_Secs,

       SUM (FlowCtlTime / 1000) AS Sum_FlowCtlTime_Secs,

       CAST(LogDate AS DATE FORMAT 'e4') (CHAR (3)) (NAMED Day_Y)

  FROM pdcrinfo.ResUsageSawt_hst a

 INNER JOIN pdcrinfo.CALENDAR b
    ON calendar_date = LogDate

 INNER JOIN pdcrinfo.Shifthour sh
    ON (EXTRACT (HOUR
                                              FROM a.TheTime)(TITLE 'Hour')) = sh.shifthour

 WHERE logdate BETWEEN dAtE'2026-04-01' AND dAtE'2026-04-30'

 GROUP BY 1,
          2,
          3,
          4,
          5

 ORDER BY 1,
          2,
          3,
          4,
          5 ;