-- =============================================================================
-- Component   : System Details
-- =============================================================================
-- Description : Reports system hardware details including node configuration,
--               CPU capacity, space capacity, and memory metrics
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT	 LogDate, NodeType, NodeCPUs, Nodes, (Nodes * NodeCPUs * 3600) AS CPUSec_Hr,
		CPUSec_Hr * 24 AS SystemCPU, 
CASE	
	WHEN NodeType LIKE '%No_AMP%' THEN 0 
 ELSE SystemCPU * .85 
END	  AS UserCPU, AMPs, PEs, SpaceCapacity_TB, SpaceUsed_TB, Nodes,
		AMPs, PEs 
FROM( 
 SELECT  a.LogDate, NodeType, NodeCPUs, Nodes, (Nodes * NodeCPUs * 3600) AS CPUSecPerHr,
		CPUSecPerHr * 24 AS SystemCPUPerDay, 
 CASE 
	WHEN NodeType LIKE '%No_AMP%' THEN 0 
	ELSE SystemCPUPerDay * .85 
END	AS UserCPU, PM_COD_CPU as PM_COD_CPU_Pct, WM_COD_CPU as WM_COD_CPU_Pct,
		
 CASE 
	WHEN ROW_NUMBER() OVER(
ORDER BY UserCPU DESC) = 1 THEN SpaceCapacity_TB 
	ELSE '-' 
END(DECIMAL(18, 2)) SpaceCapacity_TB, 
 CASE 
	WHEN ROW_NUMBER() OVER(
ORDER BY UserCPU DESC) = 1 THEN SpoolSpaceMark_TB  
	ELSE '-' 
END(DECIMAL(18, 2)) SpoolSpaceMark_TB, 
 CASE 
	WHEN ROW_NUMBER() OVER(
ORDER BY UserCPU DESC) = 1 THEN SpaceUsed_TB 
	ELSE '-' 
END(DECIMAL(18, 2)) SpaceUsed_TB, 
 CASE 
	WHEN ROW_NUMBER() OVER(
ORDER BY UserCPU DESC) = 1 THEN Eff_SpaceUsed_Pct  
	ELSE '-' 
END(DECIMAL(18, 2)) Eff_SpaceUsed_Pct, Gateways, MemSize_GB, 
Cast(AMPSpace_GB AS Format 'GzzzzzzzzzzzzzD99')(VARCHAR(30)) as AMPSpace_GB,
		cast((AMPs * AMPSpace_GB) / 1024  AS Format 'GzzzzzzzzzzzzzD99') AS SystemSpace_TB,
		
 NodeAMPs, NodePEs, AMPs, PEs, PM_COD_IO as PM_COD_IO_Pct, WM_COD_IO as WM_COD_IO_Pct 
FROM(
SELECT	 DATE - 1 LogDate, NodeType, NCPU AS NodeCPUs, COUNT(DISTINCT(dt.NodeID)) AS Nodes,
		
 MAX(dt.AMPs) * Nodes AS AMPs, MAX(dt.PEs) * Nodes AS PEs, MAX(dt.GTW) * Nodes AS Gateways,
		MAX(MemSize) / 1024**3 AS MemSize_GB, MAX(AMPSize) / 1024 **3 AS AMPSpace_GB,
		
 MAX(dt.AMPs) AS NodeAMPs, MAX(dt.PEs) AS NodePEs, max(PM_COD_CPU) PM_COD_CPU,
		max(WM_COD_CPU) WM_COD_CPU, max(PM_COD_IO) PM_COD_IO, max(WM_COD_IO) WM_COD_IO 
FROM(
SELECT	   PMA.LogDate, NodeID, 
 PM_COD_CPU, PM_COD_IO, WM_COD_CPU, WM_COD_IO, 
CASE	 
	WHEN AMPS > 0 THEN Model 
	ELSE Model || 'No_AMP'  
END	AS NodeType, NCPU, AMPs, PEs, GTW, MemSize, 
CASE 
	WHEN AMPS > 0 THEN MaxPerm_ 
	ELSE 0 
END	AS AMPSize 
FROM(
SELECT	   DISTINCT(NodeID) AS NodeId, Nodetype AS Model, 
case	
	when SpareInt=0 then NCPUs 
	else SpareInt 
end	AS NCPU, VPROC1 AS AMPs, VPROC2 AS PEs, VPROC3 AS GTW, MemSize AS MemSize,
		
thedate AS Logdate, PM_COD_CPU / 10 as PM_COD_CPU, PM_COD_IO, WM_COD_CPU / 10 as WM_COD_CPU,
		WM_COD_IO   
FROM	  DBC.ResUsageSPMA 
WHERE	 Logdate = DATE - 1) PMA INNER JOIN( 
SELECT  DATE - 1 AS Logdate, VPROC, SUM(CURRENTPERM) AS CurrentPerm,
		SUM(PEAKPERM) AS PeakPerm, SUM(MAXPERM) AS MaxPerm_, (AVG(a.CURRENTPERM) / NULLIFZERO(MAX(a.CURRENTPERM))) * 100 AS CurrentPermSkew,
		
(SUM(CURRENTPERM) / NULLIFZERO(SUM(MAXPERM))) * 100 AS PermPctUsed 
FROM	 DBC.DISKSPACE a 
WHERE	  a.maxPERM > 0 
GROUP BY 1, 2) DS 
	ON PMA.Logdate = DS.Logdate 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13) dt 
 GROUP BY 1, 2, 3) a, (
SELECT	   DATE - 1 LogDate, SUM(MAXPERM) / 1024 **4 AS SpaceCapacity_TB,
		SUM(CURRENTPERM) / 1024 **4 AS SpaceUsed_TB, SpaceCapacity_TB * 0.70 SpoolSpaceMark_TB,
		SpaceUsed_TB / SpoolSpaceMark_TB * 100 AS Eff_SpaceUsed_Pct 
 FROM       DBC.DISKSPACE 
WHERE	     maxPERM > 0) b  
WHERE	  a.Logdate = b.LogDate) dt1