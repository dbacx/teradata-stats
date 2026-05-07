-- =============================================================================
-- Component   : Sqltextinfo033
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT Month_of_Year,
       TotalCurPct,

       CASE WHEN TotalCurPct <= 50                THEN 'Healthy'
            WHEN TotalCurPct BETWEEN 50.01 AND 70 THEN 'Degraded'
            ELSE 'Critical'

             END AS SpaceHealth,
       'Space' AS SPACE

  FROM SPACE

 GROUP BY 1,
          2,
          3

 ORDER BY Month_of_Year ;