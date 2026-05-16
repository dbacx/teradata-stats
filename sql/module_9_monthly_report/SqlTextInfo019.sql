-- =============================================================================
-- Component   : Sqltextinfo019
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT logdate,
       Errorcode,
       errortext,
       wdname,
       username,
       statementtype,
       AppID,
       SUM (AMPCPUTime) AS TotalCPU,

       COUNT (QueryID) AS TotalQrys,

       errortext AS AbortedBy,

       Logdate

  FROM pdcrinfo.DBQLogTbl_Hst

 WHERE LogDate BETWEEN '2026-04-01' AND '2026-04-30'

   AND AbortFlag = 'T'

   AND ERRORCODE IN (2938, 3134, 3156, 3917, 2646, 2631, 3514)

 GROUP BY 1,
          2,
          3,
          4,
          5,
          6,
          7

 ORDER BY 1,
          2,
          3,
          4,
          5,
          6,
          7 ;