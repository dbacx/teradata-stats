-- =============================================================================
-- Component   : Query Delay Time and Volume
-- =============================================================================
-- Description : Analyzes query delay time and volume metrics by workload,
--               including response time distribution and delay percentage analysis
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING	ROW FOR ACCESS 
SELECT	Trim(Concat(Trim(Extract(Month From logDATE)), '/',Trim(Extract(DAY From logDATE)),
		'/',Trim(Extract(Year From logDATE)))) AS Logdate, WDName ,SUM(QUERY)           AS QryCnt ,
		SUM(DelayQry)        AS DelayQryCnt ,MAX(QryRT)           AS MaxQueryRT ,
		AVG(QryRT)           AS AvgQueryRT  , MIN(QryRT)           AS MinQueryRT,
		MAX(AMPCPUTIME)      AS MaxCPUTime, AVG(AMPCPUTIME)      AS AvgCPUTime,
		MIN(AMPCPUTIME)      AS MinCPUTime, SUM(AMPCPUTIME)      AS TotalCPUTime  ,
		MAX(QryRTDelayPct)  AS MaxQryRTDelayPct, AVG(QryRTDelayPct)  AS AvgQryRTDelayPct,
		MIN(QryRTDelayPct)  AS MinQryRTDelayPct, MAX(Delay)           AS MaxDelay  ,
		AVG(Delay)           AS AvgDelay, MIN(Delay)           AS MinDelay,
		AvgDelay / NULLIFZERO(AvgQueryRT) * 100 AS QryRTDelayPct, SUM(QryCntRTSec_LT10)    AS QryCntRTSec_LT10Cnt  ,
		SUM(QryCntRTSec_10_30)    AS QryCntRTSec_10_30Cnt, SUM(QryCntRTSec_30_60)    AS QryCntRTSec_30_60Cnt,
		SUM(QryCntRTMin_1_5)    AS QryCntRTMin_1_5Cnt  , SUM(QryCntRTMin_5_10)    AS QryCntRTMin_5_10Cnt,
		SUM(QryCntRTMin_10_20)    AS QryCntRTMin_10_20Cnt, SUM(QryCntRTMin_20_30)    AS QryCntRTMin_20_30Cnt  ,
		SUM(QryCntRTMin_30_60)    AS QryCntRTMin_30_60Cnt, SUM(QryCntRTMin_60_120)    AS QryCntRTMin_60_120Cnt,
		SUM(QryCntRTMin_GT120)    AS QryCntRTMin_GT120Cnt  , 
CASE	
	WHEN DelayQryCnt = 0 THEN 0  
	ELSE(((QryCntRTSec_LT10Cnt(DEC(15, 2))) / DelayQryCnt) * 100) 
end	AS QryCntRTSec_LT10Pct  , 
CASE	
	WHEN DelayQryCnt = 0 THEN 0  
	ELSE(((QryCntRTSec_10_30Cnt(DEC(15, 2))) / DelayQryCnt) * 100) 
end	AS QryCntRTSec_10_30Pct  , 
CASE	
	WHEN DelayQryCnt = 0 THEN 0  
	ELSE(((QryCntRTSec_30_60Cnt(DEC(15, 2))) / DelayQryCnt) * 100) 
end	AS QryCntRTSec_30_60Pct  , 
CASE	
	WHEN DelayQryCnt = 0 THEN 0  
	ELSE(((QryCntRTMin_1_5Cnt(DEC(15, 2))) / DelayQryCnt) * 100) 
end	AS QryCntRTMin_1_5Pct  , 
CASE	
	WHEN DelayQryCnt = 0 THEN 0  
	ELSE(((QryCntRTMin_5_10Cnt(DEC(15, 2))) / DelayQryCnt) * 100) 
end	AS QryCntRTMin_5_10Pct  , 
CASE	
	WHEN DelayQryCnt = 0 THEN 0  
	ELSE(((QryCntRTMin_10_20Cnt(DEC(15, 2))) / DelayQryCnt) * 100) 
end	AS QryCntRTMin_10_20Pct  , 
CASE	
	WHEN DelayQryCnt = 0 THEN 0  
	ELSE(((QryCntRTMin_20_30Cnt(DEC(15, 2))) / DelayQryCnt) * 100) 
end	AS QryCntRTMin_20_30Pct  , 
CASE	
	WHEN DelayQryCnt = 0 THEN 0  
	ELSE(((QryCntRTMin_30_60Cnt(DEC(15, 2))) / DelayQryCnt) * 100) 
end	AS QryCntRTMin_30_60Pct   , 
CASE	
	WHEN DelayQryCnt = 0 THEN 0  
	ELSE(((QryCntRTMin_60_120Cnt(DEC(15, 2))) / DelayQryCnt) * 100) 
end	AS QryCntRTMin_60_120Pct  , 
CASE	
	WHEN DelayQryCnt = 0 THEN 0  
	ELSE(((QryCntRTMin_GT120Cnt(DEC(15, 2))) / DelayQryCnt) * 100) 
end	AS QryCntRTMin_GT120Pct  
FROM(
SELECT	Logdate  , EXTRACT(HOUR FROM ql.starttime)  AS LogHour,
		WDName, 1 AS QUERY, 
CASE	
	WHEN DelayTime IS NOT NULL THEN 1 
	ELSE 0 
End	AS DelayQry  , DelayTime  AS Delay, (EXTRACT(SECOND FROM firstresptime) + (EXTRACT(MINUTE FROM firstresptime) * 60) + (EXTRACT(HOUR FROM firstresptime) * 3600) + (86400 * (CAST(firstresptime AS DATE) - CAST(firststeptime AS DATE)))) -  (EXTRACT(SECOND FROM firststeptime) + (EXTRACT(MINUTE FROM firststeptime) * 60) + (EXTRACT(HOUR FROM firststeptime) * 3600))(DEC(15,
		2))  AS QryRT  , AMPCPUTIME AS AMPCPUTIME, DelayTime / NULLIFZERO(QryRT) * 100 AS QryRTDelayPct,
		CASE 
	WHEN DelayTime LT 10 THEN 1 
	ELSE 0 
end	                            AS QryCntRTSec_LT10  , 
CASE	
	WHEN DelayTime GE 10 
	AND DelayTime LT 30 THEN 1 
	ELSE 0 
end	        AS QryCntRTSec_10_30, 
CASE	
	WHEN DelayTime GE 30 
	AND DelayTime LT 60 THEN 1 
	ELSE 0 
end	        AS QryCntRTSec_30_60  , 
CASE	
	WHEN DelayTime GE 60 
	AND DelayTime LT 300 THEN 1 
	ELSE 0 
end	       AS QryCntRTMin_1_5, 
CASE	
	WHEN DelayTime GE 300 
	AND DelayTime LT 600 THEN 1 
	ELSE 0 
end	      AS QryCntRTMin_5_10  , 
CASE	
	WHEN DelayTime GE 600 
	AND DelayTime LT 1200 THEN 1 
	ELSE 0 
end	     AS QryCntRTMin_10_20, 
CASE	
	WHEN DelayTime GE 1200 
	AND DelayTime LT 1800 THEN 1 
	ELSE 0 
end	    AS QryCntRTMin_20_30  , 
CASE	
	WHEN DelayTime GE 1800 
	AND DelayTime LT 3600 THEN 1 
	ELSE 0 
end	    AS QryCntRTMin_30_60, 
CASE	
	WHEN DelayTime GE 3600 
	AND DelayTime LT 7200 THEN 1 
	ELSE 0 
end	    AS QryCntRTMin_60_120  , 
CASE	
	WHEN DelayTime GE 7200 THEN 1 
	ELSE 0 
end	      AS QryCntRTMin_GT120 
FROM	 pdcrinfo.DBQLogTblRpt_hst  ql 
WHERE	ql.Logdate BETWEEN '2024-07-01' 
	AND '2024-07-31' 
	AND ql.WDName IS NOT NULL  
	AND ql.AMPCPUTime > 0) ql INNER JOIN pdcrinfo.CALENDAR c 
	ON ql.logdate = c.calendar_date INNER JOIN pdcrinfo.ShiftHour s 
	ON  LogHour = s.shifthour  
WHERE	c.calendar_date  BETWEEN DATE'2024-07-01' 
	AND DATE'2024-07-31' 
GROUP BY 1, 2;