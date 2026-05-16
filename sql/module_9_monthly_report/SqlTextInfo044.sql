-- =============================================================================
-- Component   : Sqltextinfo044
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT 'Space Consumed in TB',
       SUM (currentperm) / 1024 ** 4

  FROM dbc.diskspace

UNION ALL
SELECT 'CPU Utilization',
       SUM (AMPCPUTime + ParserCPUTime) (NAMED TotalCPU)

  FROM pdcrinfo.DBQLOGTBL_HST

 WHERE LOGDATE BETWEEN dAtE'2026-04-01' AND '2026-04-30'

 ORDER BY 1 DESC ;