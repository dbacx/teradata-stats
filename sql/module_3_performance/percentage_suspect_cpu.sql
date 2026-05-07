-- =============================================================================
-- Component   : Percentage of Total CPU that is Suspect
-- =============================================================================
-- Description : Calculates the percentage of total CPU consumed by suspect
--               queries (queries with abnormal CPU/IO ratios or skew)
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT	LogDate, SUM(AMPCPUTime+Parsercputime) SumCPU  , SUM(
CASE	
	WHEN (TotalIOCount>0 
	AND ((AMPCPUTime+Parsercputime) * 1000)/TotalIOCount > 3) 
	OR((AMPCPUTime + Parsercputime) > 0 
	AND((TotalIOCount) / ((AMPCPUTime + Parsercputime) * 1000) > 3)) 
	OR(((AMPCPUTime + Parsercputime) / (HASHAMP() + 1)) > 0 
	AND(1 - (AmpCPUTime / (HASHAMP() + 1)) / NULLIFZERO(MaxAmpCPUTime)) > 0.5) 
	OR((TotalIOCount / (HASHAMP() + 1)) > 0 
	AND(1 - (TotalIOCount / (HASHAMP() + 1)) / NULLIFZERO(MaxAmpIO)) > 0.5) THEN(AMPCPUTime + Parsercputime) 
	ELSE 0 
END) SuspectCPU , MAX(SystemCPUPerNode) SystemCPUPerNode, MAX(TotalSystemCPU) TotalSystemCPU 
FROM	   pdcrinfo.dbqlogtbl_hst a, (
SELECT	NodeType, NCPUs*86400 AS SystemCPUPerNode, COUNT(DISTINCT(NodeID)) AS Nodes,
		SystemCPUPerNode * Nodes AS TotalSystemCPU  
FROM	DBC.ResUsageSPMA 
WHERE	thedate = DATE - 1   
	AND vproc1 > 0    
GROUP BY 1, 2 ) cpuinfo 
WHERE	logdate BETWEEN '2024-07-01' 
	AND  '2024-07-31' 
GROUP BY 1;