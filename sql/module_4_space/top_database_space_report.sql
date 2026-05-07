-- =============================================================================
-- Component   : Top Database Space Report
-- =============================================================================
-- Description : Reports top 10 databases by current perm space utilization
--               including space metrics and skew information
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCK	ROW ACCESS 
SELECT	CURRENTPERMRnk , year_of_calendar, Month_of_Year, Week_of_year,
		LogDate, DatabaseName, AccountName, CURRENTPERM, CURRENTPERM / (1024 * 1024 * 1024) AS CURRENTPERM_Gb,
		PEAKPERM, MAXPERM , MAXPERM / (1024 * 1024 * 1024) AS MAXPERM_Gb ,
		CURRENTPERMSKEW, PERMPCTUSED 
FROM(
SELECT	Rank()  Over(
ORDER BY CURRENTPERM DESC) AS CURRENTPERMRnk, c.year_of_calendar,
		c.Month_of_Year, c.Week_of_year, LogDate, DatabaseName, AccountName,
		CURRENTPERM, CURRENTPERM / (1024 * 1024 * 1024) AS CURRENTPERM_Gb ,
		PEAKPERM, MAXPERM, MAXPERM / (1024 * 1024 * 1024) AS MAXPERM_Gb,
		CURRENTPERMSKEW, PERMPCTUSED 
FROM	PDCRINFO.DatabaseSpace_Hst a INNER JOIN PDCRINFO.CALENDAR c 
	ON a.Logdate = c.Calendar_date 
WHERE	 c.Calendar_date = a.Logdate 
	AND a.Logdate = (
SELECT	Max(Logdate) 
FROM	PDCRINFO.DatabaseSpace_Hst)) dt 
WHERE	CURRENTPERMRnk <= 10;