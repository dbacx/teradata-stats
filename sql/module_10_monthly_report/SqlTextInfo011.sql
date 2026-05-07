-- =============================================================================
-- Component   : Sqltextinfo011
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT NodeType,
       NCPUs * 86400 * .8 AS UserCPUPerNode,
       COUNT (DISTINCT (NodeID)) AS Nodes,

       UserCPUPerNode * Nodes AS TotalUserCPU

  FROM DBC.ResUsageSPMA

 WHERE thedate = DATE

   AND vproc1 > 0

 GROUP BY 1,
          2 ;