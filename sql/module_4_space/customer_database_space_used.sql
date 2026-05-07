-- =============================================================================
-- Component   : Customer Database Space Used
-- =============================================================================
-- Description : Analyzes database space utilization by customer/organization,
--               including current perm, peak perm, max perm, and skew metrics
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCK	ROW ACCESS  
SELECT	Year_of_Calendar ,Month_of_Year ,Week_of_year ,Day_of_Month ,
		Day_of_Week ,(dtall.LogDate - ((dtall.LogDate - DATE '0001-01-07') MOD 7)) AS BOW   ,
		 dtall.LogDate, info.Organization, info.Department, info.GroupName,
		DATABASES, CURRENTPERM, PEAKPERM, MAXPERM, CURRENTPERMSKEW, PERMPCTUSED ,
		CURRENTPERM/1024**4 CURRENTPERM_TB   
FROM(
SELECT	 LogDate, DATABASES, CURRENTPERM, PEAKPERM, MAXPERM, CURRENTPERMSKEW,
		PERMPCTUSED, DBSYSPCT  
FROM(
SELECT	   dt.LogDate,  
CASE	    
	WHEN dt.CURRENTPERM > 50000000000 THEN dt.DatabaseName  
	ELSE 'LT50GBDatabases'  
END	   AS DATABASES, MAX(dt.CURRENTPERM)  AS CURRENTPERM, MAX(dt.PEAKPERM)        AS PEAKPERM,
		MAX(dt.MAXPERM)         AS MAXPERM, 
CASE	    
	WHEN dt.CURRENTPERM > 50000000000 THEN MAX(dt.CURRENTPERMSKEW)  
	ELSE AVG(dt.CURRENTPERMSKEW)  
END	  AS CURRENTPERMSKEW, 
CASE	  
	WHEN dt.CURRENTPERM > 50000000000 THEN MAX(dt.PERMPCTUSED)  
	ELSE AVG(dt.PERMPCTUSED)  
END	  AS PERMPCTUSED, SUM(dt.CURRENTPERM) / NullIfZero(MAX(sys.TotalMaxPerm) * 100) AS DBSYSPCT  
FROM(
SELECT	  LogDate,  DatabaseName, CURRENTPERM, PEAKPERM, MAXPERM,
		CURRENTPERMSKEW, PERMPCTUSED  
FROM	 pdcrinfo.DatabaseSpace_Hst  
WHERE	  logdate BETWEEN  DATE'2024-07-01' 
	AND DATE'2024-07-31') dt INNER JOIN(  
SELECT	 LogDate, SUM(MAXPERM) AS TotalMaxPerm   
FROM	   pdcrinfo.DatabaseSpace_Hst  
WHERE	  logdate BETWEEN  DATE'2024-07-01' 
	AND DATE'2024-07-31'  
GROUP BY 1) sys  
	ON dt.logdate = sys.logdate  
GROUP BY 1, 2) db1   
UNION	   ALL  
SELECT	dbtot.Logdate, dbtot.Databases  AS DATABASES, dbtot.CURRENTPERM,
		 NULL AS PEAKPERM, NULL AS MAXPERM, NULL AS CURRENTPERMSKEW,
		NULL AS PERMPCTUSED, NULL AS DBSYSPCT  
FROM(
SELECT	   LogDate, 'SYS_SPOOL_PERM'       AS Databases, Sum(Cast(MaxPerm AS DECIMAL(32,
		0))) * 0.7 AS CURRENTPERM  
FROM	   pdcrinfo.DataBaseSpace_Hst  
WHERE	  logdate BETWEEN  DATE'2024-07-01' 
	AND DATE'2024-07-31'  
GROUP BY 1, 2  
UNION	  ALL  
SELECT	LogDate, 'SYS_MAX_PERM'               AS Databases, Sum(Cast(MaxPerm AS DECIMAL(32,
		0))) AS CURRENTPERM  
FROM	   pdcrinfo.DataBaseSpace_Hst  
WHERE	  logdate BETWEEN  DATE'2024-07-01' 
	AND DATE'2024-07-31'  
GROUP BY 1, 2) dbtot) dtall INNER JOIN pdcrinfo.CALENDAR c  
	ON dtall.Logdate = c.Calendar_date  LEFT OUTER JOIN pdcrinfo.SpaceOwnerInfo info  
	ON dtall.DATABASES = info.DatabaseName  
WHERE	c.Calendar_date BETWEEN  DATE'2024-07-01' 
	AND DATE'2024-07-31'  
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 ;