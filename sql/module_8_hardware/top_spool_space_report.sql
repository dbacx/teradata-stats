-- =============================================================================
-- Component   : Top Spool Space Report
-- =============================================================================
-- Description : Reports top 10 users by peak spool space utilization
--               including spool metrics and skew information
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCK	ROW ACCESS 
SELECT	PeakSpoolRnk, dt.year_of_calendar, dt.Month_of_Year, dt.Week_of_year,
		LogDate, UserName, AccountName, CURRENTSPOOL , PEAKSPOOL , MAXSPOOL,
		MAXSPOOL / (1024 * 1024 * 1024) AS MAXSPOOL_Gb, PEAKSPOOLSKEW 
FROM	(
SELECT	Rank()  Over(
ORDER BY PEAKSPOOL DESC) AS PeakSpoolRnk, c.year_of_calendar ,
		c.Month_of_Year , c.Week_of_year, LogDate, UserName , AccountName,
		CURRENTSPOOL , PEAKSPOOL , MAXSPOOL , MAXSPOOL / (1024 * 1024 * 1024) AS MAXSPOOL_Gb ,
		PEAKSPOOLSKEW 
FROM	PDCRINFO.SpoolSpace_hst a INNER JOIN PDCRINFO.CALENDAR c 
	ON a.Logdate = c.Calendar_date 
WHERE	 c.Calendar_date = a.Logdate 
	AND a.Logdate = (
SELECT	Max(Logdate) 
FROM	PDCRINFO.SpoolSpace_hst) ) dt 
WHERE	PeakSpoolRnk <= 10;