-- =============================================================================
-- Component   : Sqltextinfo069
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
SELECT LogDate,
       SUM (CASE
                         WHEN (TotalIOCount > 0
                               AND ((AMPCPUTime + Parsercputime) * 1000) / TotalIOCount > 3)
                              OR ((AMPCPUTime + Parsercputime) > 0
                                  AND ((TotalIOCount) / ((AMPCPUTime + Parsercputime) * 1000) > 3))
                              OR (((AMPCPUTime + Parsercputime) / (HASHAMP () + 1)) > 0
                                  AND (1 - (AmpCPUTime / (HASHAMP () + 1)) / NULLIFZERO (MaxAmpCPUTime)) > 0.5)
                              OR ((TotalIOCount / (HASHAMP () + 1)) > 0
                                  AND (1 - (TotalIOCount / (HASHAMP () + 1)) / NULLIFZERO (MaxAmpIO)) > 0.5) THEN (AMPCPUTime + Parsercputime) ELSE 0
                     END) (NAMED SuspectCPU) , (SuspectCPU / NULLIFZERO (SUM (AMPCPUTime + Parsercputime))) * 100 AS SuspectCPUPct

  FROM PDCRInfo.dbqlogtbl_hst a

 WHERE logdate >= DATE - 90

 GROUP BY 1 ;