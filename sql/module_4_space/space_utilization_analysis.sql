-- =============================================================================
-- Component   : Space Utilization Analysis
-- =============================================================================
-- Description : Analyzes overall space utilization metrics including total max perm,
--               total current perm, and space health classification
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

CREATE	VOLATILE TABLE space, NO Log AS(
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
                           
END	Month_of_Year, Max(dt1.TotalMaxPerm) / (1024 * 1024 * 1024 * 1024)   AS TotalMaxPerm_Tb,
		Max(dt1.TotalCurPerm) / (1024 * 1024 * 1024 * 1024)   AS TotalCurPerm_Tb,
		Max(dt1.TotalCurPct)   AS TotalCurPct, Max(dt1.TotalAvailPct)   AS TotalAvailPct
                             
FROM	(
SELECT	LogDate, DatabaseName, CURRENTPERM, PEAKPERM, MAXPERM, CURRENTPERMSKEW,
		PERMPCTUSED 
FROM	PDCRINFO.DatabaseSpace_Hst 
WHERE	Logdate BETWEEN DATE - 61 
	AND DATE - 1) dt INNER JOIN (
SELECT	LogDate,Sum(MAXPERM) AS TotalMaxPerm, Sum(CURRENTPERM) AS TotalCurPerm,
		Sum(PEAKPERM) AS TotalPeakPerm, TotalMaxPerm - TotalCurPerm AS TotalAvailPerm,
		TotalCurPerm / TotalMaxPerm * 100   AS TotalCurPct
                           , TotalAvailPerm / TotalMaxPerm * 100 AS TotalAvailPct 
FROM	PDCRINFO.DatabaseSpace_Hst 
WHERE	Logdate BETWEEN DATE - 61 
	AND DATE - 1 
GROUP BY 1) dt1 
	ON dt.logdate = dt1.logdate
                             INNER JOIN PDCRINFO.CALENDAR c 
	ON dt.Logdate = c.Calendar_date LEFT OUTER JOIN PDCRINFO.SpaceOwnerInfo info 
	ON dt.DatabaseName = info.DatabaseName 
WHERE	c.Calendar_date BETWEEN   '2024-07-01' and '2024-09-30'
GROUP BY 1)
WITH	DATA NO PRIMARY INDEX 
	ON 
COMMIT	PRESERVE ROWS;
SELECT	Month_of_Year, TotalCurPct, 
CASE	
	WHEN TotalCurPct <= 50 THEN 'Healthy' 
	WHEN TotalCurPct BETWEEN 50.01 
	AND 70 THEN 'Degraded' 
	ELSE 'Critical' 
End	AS SpaceHealth,'Space' as Space 
FROM	space 
GROUP BY 1, 2, 3 
order by Month_of_Year;
drop	table space;