-- =============================================================================
-- Component   : IO Heatmap
-- =============================================================================
-- Description : Generates I/O utilization heatmap by hour of day to identify
--               disk I/O patterns and peak usage times
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT	a.thedate(DATE)(FORMAT 'yyyy-mm-dd')(CHAR(10)) as TheDate,
		Cast(thedate AS DATE Format'e4') (CHAR(3)) DoW, cast(AVG(
CASE	
	WHEN  A.LogHour = '00' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _0_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '01' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _1_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '02' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _2_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '03' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _3_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '04' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _4_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '05' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _5_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '06' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _6_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '07' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _7_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '08' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _8_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '09' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _9_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '10' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _10_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '11' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _11_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '12' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _12_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '13' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _13_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '14' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _14_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '15' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _15_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '16' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _16_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '17' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _17_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '18' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _18_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '19' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _19_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '20' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _20_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '21' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _21_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '22' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _22_,   cast(AVG(
CASE	
	WHEN  A.LogHour = '23' THEN((A.bandwidth / NULLIFZERO(A.spare02)) * 100) 
END) as int) AS _23_   
FROM(
SELECT	thedate, (extract(hour from TheTime)(format '99')(Char(2))) as LogHour,
		nodeid, Spare02, max(wm_cod_cpu) cpu_Cod,   AVG(((CPUUServ + CPUUExec) / NULLIFZERO(NCPUs)) / Secs) AS AvgCPUPct,
		MAX(((CPUUServ + CPUUExec) / NULLIFZERO(NCPUs)) / Secs) AS MaxCPUPct,
		  avg(zeroifnull((CPUUExec + CPUUServ)) / NullifZero((CPUUExec + CPUUServ + CPUIoWait + CPUIdle))) avgCPUBusy,
		((SUM(FileAcqReadKB + FilePreReadKB) * 100 / SUM(NULLIFZERO(CentiSecs)) + SUM(FileWriteKB) * 100 / SUM(NULLIFZERO(CentiSecs))) / 1024) bandwidth   
FROM	pdcrinfo.ResUsageSpma_hst 
WHERE	thedate BETWEEN DATE'2024-07-01'  
	and  DATE'2024-07-31' 
GROUP BY 1, 2, 3, 4) a 
group by 1,2 
order by 1;