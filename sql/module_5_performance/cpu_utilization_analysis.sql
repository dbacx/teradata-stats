-- =============================================================================
-- Component   : CPU Utilization Analysis
-- =============================================================================
-- Description : Analyzes CPU utilization metrics including user CPU, system CPU,
--               AMP CPU, PE CPU, CPU skew, and overall health classification
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

 CREATE VOLATILE TABLE CPU, NO Log AS (
SELECT	c.year_of_calendar,c.Month_of_Year, c.Week_of_Year, (LogDate - ((LogDate - DATE '0001-01-07') MOD 7)) AS BOW,
		c.Day_of_Month,c.Day_of_week,a.LogDate,sh.WorkPeriod,sh.PERIOD,
		a.Loghour,a.NodeType AS NodeType

                              ,Count(DISTINCT(Nodeid)) AS Nodes,
		Sum(Secs)AS Seconds,Sum(CPUServSec + CPUExecSec + CPUWaitIOSec + CPUIdleSec) AS CPUTotal,
		Sum(CPUServSec + CPUExecSec) AS CPUBusy, Sum(CPUServSec) AS CPUOS,
		Sum(CPUExecSec) AS CPUUser, Sum(CPUWaitIOSec) AS CPUWaitIO, Sum(CPUIdleSec) AS CPUIdle,
		Sum(AMPTotalUserExec + AMPTotalUserServ) AS AMPCPU, Sum(PETotalUserExec + PETotalUserServ) AS PECPU
                              , CPUBUSY - (AMPCPU + PECPU) AS CPUOverHead,
		(CPUBUSY / NullIfZero(CPUTotal)) * 100 AS CPUUtilPct, (CPUOS / NullIfZero(CPUTotal)) * 100 AS CPUOSPct,
		(CPUUser / NullIfZero(CPUTotal)) * 100 AS CPUUserPct, (CPUWaitIO / NullIfZero(CPUTotal)) * 100 AS CPUWaitIOPct,
		(CPUIdle / NullIfZero(CPUTotal)) * 100  AS CPUIdlePct, (1 - ((AMPCPU + PECPU) / NullIfZero(CPUBUSY))) * 100  AS CPUOvrHdPct ,
		(AMPCPU / NullIfZero(CPUBUSY)) * 100 AS CPUAMPPct , (PECPU / NullIfZero(CPUBUSY)) * 100  AS CPUPEPct,
		(CPUUser / NullIfZero(CPUBUSY)) * 100 AS BusyUserPct, (CPUOS / NullIfZero(CPUBUSY)) * 100  AS BusyOSPct
                              , (Avg(CPUServSec + CPUExecSec) * 1000) / NullIfZero(Avg(IOReadCount + IOWriteCount))  AS AVGCPUIORatio,
		(Max(CPUServSec + CPUExecSec) * 1000) / NullIfZero(Max(IOReadCount + IOWriteCount))  AS MAXCPUIORatio,
		((1 - (Avg(CPUServSec + CPUExecSec) / NullIfZero(Max(CPUServSec + CPUExecSec)))) * 100)    AS CPUBusySkew,
		((1 - (Avg(CPUServSec) / NullIfZero(Max(CPUServSec)))) * 100) AS CPUOSSkew,
		((1 - (Avg(CPUExecSec) / NullIfZero(Max(CPUExecSec)))) * 100) AS CPUUserSkew,
		((1 - (Avg(CPUWaitIOSec) / NullIfZero(Max(CPUWaitIOSec)))) * 100) AS CPUWaitIOSkew 
FROM	PDCRINFO.ResUsageSum10_hst a  INNER JOIN PDCRINFO.CALENDAR c 
	ON a.logdate = c.calendar_date INNER JOIN  PDCRINFO.Shifthour sh 
	ON a.loghour = sh.shifthour 
WHERE	 a.LogDate   BETWEEN  '2024-08-01' and '2024-09-30'
	AND  c.Calendar_date   BETWEEN  '2024-07-01' and '2024-09-30'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
WITH	DATA PRIMARY INDEX (CPUUtilPct) 
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
END	Month_of_Year, Avg(CPUUtilPct)AS Avg_CpuUtil, 
CASE	
	WHEN Avg_CpuUtil BETWEEN 0 AND 50 THEN 'Healthy' 
	WHEN Avg_CpuUtil BETWEEN 50.01 
	AND 60 THEN 'Busy' 
	WHEN Avg_CpuUtil BETWEEN 60.01 
	AND 80 THEN 'Very Busy' 
	ELSE 'Critical' 
END	AS CPU_Notation,'CPU' as CPU 
FROM	CPU 
GROUP BY Month_of_Year 
order by Month_of_Year;
drop	table CPU;