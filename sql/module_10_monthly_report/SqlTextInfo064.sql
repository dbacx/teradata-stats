-- =============================================================================
-- Component   : Sqltextinfo064
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
SELECT logdate,
       cpu_utilize,
       cpubusy (NAMED cpu_count)

  FROM

        (SELECT logdate(FORMAT 'yyyy-mm-dd') (NAMED logdate) , (CPUBUSY / NULLIFZERO (CPUTotal)) * 100 AS cpu_utilize,
               SUM (CPUServSec + CPUExecSec + CPUWaitIOSec + CPUIdleSec) AS CPUTotal,

               SUM (CPUServSec + CPUExecSec) AS CPUBusy

          FROM PDCRINFO.ResUsageSum10_hst a

         WHERE logdate >= DATE - 90

         GROUP BY logdate
       ) a ;