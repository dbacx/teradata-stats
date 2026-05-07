-- =============================================================================
-- Component   : Space Capacity Forecast
-- =============================================================================
-- Description : Analyzes space capacity trends over time to forecast
--               space utilization and identify capacity planning needs
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

 SELECT year_of_calendar, 
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
END	Month_of_Year,Trim(
Case	Month_of_Year  
	WHEN 1 THEN 'Jan' 
	WHEN 2 THEN 'Feb' 
	WHEN 3 THEN 'Mar' 
	WHEN 4 THEN 'Apr' 
	WHEN 5 THEN 'May' 
	WHEN 6 THEN 'Jun' 
	WHEN 7 THEN 'Jul' 
	WHEN 8 THEN 'Aug' 
	WHEN 9 THEN 'Sep' 
	WHEN 10 THEN 'Oct' 
	WHEN 11 THEN 'Nov' 
	WHEN 12 THEN 'Dec' 
end) || '-' || Trim(year_of_calendar) as Month_Year, Week_of_year ,
		Day_of_month, (dt.LogDate - ((dt.LogDate - DATE '0001-01-07') MOD 7)) AS BOW,
		dt.LogDate,info.Organization ,info.Department,info.GroupName,
		CASE 
	WHEN dt.CURRENTPERM > 50000000000 THEN dt.DatabaseName 
	ELSE 'LT50GBDatabases' 
END	Databases, Sum(dt.CURRENTPERM)AS CURRENTPERM, Sum(dt.PEAKPERM) AS PEAKPERM,
		Sum(dt.MAXPERM) AS MAXPERM , 
CASE	
	WHEN dt.CURRENTPERM > 50000000000 THEN Max(dt.CURRENTPERMSKEW) 
	ELSE Avg(dt.CURRENTPERMSKEW) 
END	AS CURRENTPERMSKEW , 
CASE	
	WHEN dt.CURRENTPERM > 50000000000 THEN Max(dt.PERMPCTUSED) 
	ELSE Avg(dt.PERMPCTUSED) 
END	AS PERMPCTUSED , Sum(dt.CURRENTPERM) / Max(dt1.TotalMaxPerm) * 100 AS DBSYSPCT,
		Max(dt1.TotalMaxPerm)   AS TotalMaxPerm, Max(dt1.TotalMaxPerm) / (1024 * 1024 * 1024 * 1024)   AS TotalMaxPerm_Tb ,
		Max(dt1.TotalCurPerm) / (1024 * 1024 * 1024 * 1024)   AS TotalCurPerm_Tb,
		Max(dt1.TotalPeakPerm)   AS TotalPeakPerm, Max(dt1.TotalAvailPerm)  AS TotalAvailPerm,
		Max(dt1.TotalCurPct)   AS TotalCurPct, Max(dt1.TotalAvailPct)   AS TotalAvailPct 
FROM	(
SELECT	LogDate, DatabaseName, CURRENTPERM, PEAKPERM, MAXPERM, CURRENTPERMSKEW,
		PERMPCTUSED 
FROM	PDCRINFO.DatabaseSpace_Hst 
WHERE	Logdate BETWEEN DATE'2023-08-08'  
	and  DATE'2024-08-08') dt  INNER JOIN (
SELECT	LogDate, Sum(MAXPERM) AS TotalMaxPerm, Sum(CURRENTPERM) AS TotalCurPerm,
		Sum(PEAKPERM)  AS TotalPeakPerm, TotalMaxPerm - TotalCurPerm         AS TotalAvailPerm,
		TotalCurPerm / TotalMaxPerm * 100   AS TotalCurPct, TotalAvailPerm / TotalMaxPerm * 100 AS TotalAvailPct  
FROM	PDCRINFO.DatabaseSpace_Hst 
WHERE	Logdate BETWEEN DATE'2023-08-08'  
	and  DATE'2024-08-08' 
GROUP BY 1) dt1 
	ON dt.logdate = dt1.logdate INNER JOIN PDCRINFO.CALENDAR c  
	ON dt.Logdate = c.Calendar_date LEFT OUTER JOIN PDCRINFO.SpaceOwnerInfo info 
	ON dt.DatabaseName = info.DatabaseName 
WHERE	c.Calendar_date BETWEEN DATE'2023-08-08'  
	and  DATE'2024-08-08' 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11 
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11; 