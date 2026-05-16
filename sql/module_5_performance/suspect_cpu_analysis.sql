-- =============================================================================
-- Component   : Suspect CPU Analysis
-- =============================================================================
-- Description : Analyzes suspect CPU consumption by identifying queries with
--               abnormal CPU/IO ratios or skew, and classifies health status
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

CREATE	VOLATILE TABLE Suspect_Cpu, NO Log AS(
SEL	 Extract(MONTH From logdate) AS Month_of_Year, Avg(suspectCPUpct) AS Avg_suspectCPUpct
                            
FROM	(
SELECT	logdate, Extract(MONTH From logdate) AS Mnth, Avg(sumcpu) AS Avg_CPU,
		Avg(SuspectCPU)  AS AVGSuspectCPU, (AVGSuspectCPU / Avg_CPU) * 100 suspectCPUpct
                            
FROM(
SELECT	   LogDate, Sum(AMPCPUTime + Parsercputime)  SumCPU, Sum(
CASE	
	WHEN(TotalIOCount > 0 
	AND((AMPCPUTime + Parsercputime) * 1000) / TotalIOCount > 3)
	OR((AMPCPUTime + Parsercputime) > 0 
	AND((TotalIOCount) / ((AMPCPUTime + Parsercputime) * 1000) > 3)) 
	OR(((AMPCPUTime + Parsercputime) / (HashAmp() + 1)) > 0
                            
	AND (1 - (AmpCPUTime / (HashAmp() + 1)) / NullIfZero(MaxAmpCPUTime)) > 0.5) 
	OR((TotalIOCount / (HashAmp() + 1)) > 0 
	AND(1 - (TotalIOCount / (HashAmp() + 1)) / NullIfZero(MaxAmpIO)) > 0.5) THEN(AMPCPUTime + Parsercputime)
                            
	ELSE 0 
END)  SuspectCPU, Max(UserCPUPerNode) UserCPUPerNode, Max(TotalUserCPU) TotalUserCPU 
FROM	pdcrinfo.dbqlogtbl_hst a, (
SELECT	NodeType, NCPUs*86400 * .8 AS UserCPUPerNode, Count(DISTINCT(NodeID)) AS Nodes,
		UserCPUPerNode * Nodes AS  TotalUserCPU 
FROM	DBC.ResUsageSPMA 
WHERE	 thedate = DATE 
	AND vproc1 > 0 
GROUP BY 1, 2) cpuinfo
                             
WHERE	   logdate BETWEEN  '2024-07-01' and '2024-09-30'
GROUP BY 1) a 
GROUP BY 1, 2) f 
GROUP BY 1)
WITH	DATA NO PRIMARY INDEX 
	ON 
COMMIT	PRESERVE ROWS;
SELECT	
CASE	Month_of_Year 
	WHEN 1 THEN 'January' 
	WHEN 2 THEN 'February' 
	WHEN 3 THEN 'March' 
	WHEN 4 THEN 'April' 
	WHEN 5 THEN 'May' 
	WHEN 6 THEN 'June' 
	WHEN 7 THEN 'July' 
	WHEN 8 THEN 'August' 
	WHEN 9 THEN 'September' 
	WHEN 10 THEN 'October' 
	WHEN 11 THEN 'November' 
	WHEN 12 THEN 'December' 
END	Month_of_Year, Avg_suspectCPUpct, 
CASE	
	WHEN Avg_suspectCPUpct <= 25 THEN 'Healthy' 
	WHEN Avg_suspectCPUpct BETWEEN 25.01 
	AND 44 THEN 'Degraded' 
	ELSE 'Critical'
END	AS AvgSuspect_CPU,'Suspect CPU' as Suspect_CPU 
FROM	Suspect_Cpu 
GROUP BY Month_of_Year ,2,3,4 
ORDER BY Month_of_Year ASC;
drop	table Suspect_Cpu