-- =============================================================================
-- Component   : Sqltextinfo004
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT CAST(starttime AS DATE) AS logdate,
       SUM (AMPCPUtime + ParserCPUTime) AS Totl_CPU,
       SUM (MAXAMPCPUTime * Numofactiveamps) AS Impct_CPU

  FROM pdcrinfo.dbqlogtbl_hst

 WHERE logdate BETWEEN dAtE'2026-04-01' AND dAtE'2026-04-30'

 GROUP BY 1 ;