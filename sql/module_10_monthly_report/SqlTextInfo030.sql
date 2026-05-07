-- =============================================================================
-- Component   : Sqltextinfo030
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

             END (NAMED Month_of_Year) , xFlowCtlTime,

       CASE
                                       WHEN xFlowCtlTime <= 5.00
                                            OR xFlowCtlCnt < 1000.00                                   THEN 'Healthy'

            WHEN xFlowCtlTime BETWEEN 5.01 AND 180.00
                                            OR xFlowCtlCnt BETWEEN 1000.01 AND 3000.00 THEN 'Degraded'
            ELSE 'Critical'

             END AS AWT_Notation,
       'Flow Control' AS Flow_Control

  FROM

        (SELECT Month_of_Year,
               CAST(MAXIMUM (FlowCtlTime / 1000) AS FLOAT) (NAMED xFlowCtlTime) , CAST(MAXIMUM (FlowCtlCnt) AS FLOAT) (NAMED xFlowCtlCnt)

          FROM FlowControl

         GROUP BY 1
       ) x

 ORDER BY Month_of_Year ;