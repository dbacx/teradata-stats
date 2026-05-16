-- =============================================================================
-- Component   : CPU Heatmap
-- =============================================================================
-- Description : Generates CPU utilization heatmap by hour of day to identify
--               peak usage patterns and time-based performance trends
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING	ROW FOR ACCESS  
SELECT	 c.calendar_date LogDate , Cast(calendar_date AS DATE Format'e4') (CHAR(3)) DoW  ,
		 Sum(
CASE	
	WHEN loghour = 0 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _0_,  Sum(
CASE	
	WHEN loghour = 1 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _1_,  Sum(
CASE	
	WHEN loghour = 2 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _2_,  Sum(
CASE	
	WHEN loghour = 3 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _3_,  Sum(
CASE	
	WHEN loghour = 4 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _4_,  Sum(
CASE	
	WHEN loghour = 5 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _5_,  Sum(
CASE	
	WHEN loghour = 6 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _6_,  Sum(
CASE	
	WHEN loghour = 7 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _7_,  Sum(
CASE	
	WHEN loghour = 8 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _8_,  Sum(
CASE	
	WHEN loghour = 9 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _9_,  Sum(
CASE	
	WHEN loghour = 10 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _10_,  Sum(
CASE	
	WHEN loghour = 11 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _11_,  Sum(
CASE	
	WHEN loghour = 12 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _12_,  Sum(
CASE	
	WHEN loghour = 13 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _13_,  Sum(
CASE	
	WHEN loghour = 14 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _14_,  Sum(
CASE	
	WHEN loghour = 15 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _15_,  Sum(
CASE	
	WHEN loghour = 16 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _16_,  Sum(
CASE	
	WHEN loghour = 17 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _17_,  Sum(
CASE	
	WHEN loghour = 18 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _18_,  Sum(
CASE	
	WHEN loghour = 19 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _19_,  Sum(
CASE	
	WHEN loghour = 20 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _20_,  Sum(
CASE	
	WHEN loghour = 21 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _21_,  Sum(
CASE	
	WHEN loghour = 22 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _22_,  Sum(
CASE	
	WHEN loghour = 23 THEN CPUUtilPctWMCOD 
	ELSE 0 
end) AS _23_  
FROM(
SELECT	 a.LogDate, a.Loghour, Sum(TotalCPUServSec + TotalCPUExecSec) AS CPUBusy,
		 Sum(TotalCPUServSec + TotalCPUExecSec + TotalCPUWaitIOSec + TotalCPUIdleSec) AS CPUTotal,
		 (CPUBUSY / NullIfZero(CPUTotal)) * 100(DECIMAL(3, 0)) AS CPUUtilPct,
		 (Cast(CPUUtilPct AS DECIMAL(10, 3)) / (Cast(Max(WM_COD_CPU) AS DECIMAL(10,
		3)) / 10)) * 100(DECIMAL(3, 0)) CPUUtilPctWMCOD  
FROM	pdcrinfo.ResUsageSumHr_hst a   INNER JOIN pdcrinfo.ResUsageSPMA b 
	ON a.LogDate = b.TheDate 
	AND a.Loghour = Extract(HOUR From b.thetime)  
WHERE	 a.LogDate BETWEEN DATE'2024-07-01' 
	AND DATE'2024-07-31'  
GROUP BY 1, 2)a  RIGHT OUTER JOIN pdcrinfo.CALENDAR c  
	ON a.logdate = c.calendar_date  
WHERE	c.calendar_date BETWEEN DATE'2024-07-01' 
	AND DATE'2024-07-31'  
GROUP BY 1 
ORDER BY 1; 