-- =============================================================================
-- Component   : Top 20 Users by Impact CPU
-- =============================================================================
-- Description : Identifies top 20 users by impact CPU consumption across
--               multiple months to track high-impact users and trends
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT	ROW_NUMBER() OVER (
ORDER BY IC_this_Month DESC) User_Rank, USERNAME   , CAST(SUM(
CASE	
	WHEN LogMonth = (CAST((ADD_MONTHS(DATE'2024-07-31' - EXTRACT(DAY FROM DATE'2024-07-31') + 1,
		-2)) AS DATE FORMAT'mmm-yyyy')(CHAR(8))) THEN  Impact_CPU 
	ELSE 0 
END) AS FORMAT 'Gzzzzzzzzz') IC_prior_prev_Month   , CAST(SUM(
CASE	
	WHEN LogMonth = (CAST((ADD_MONTHS(DATE'2024-07-31' - EXTRACT(DAY FROM DATE'2024-07-31') + 1,
		-1)) AS DATE FORMAT'mmm-yyyy')(CHAR(8))) THEN  Impact_CPU 
	ELSE 0 
END) AS FORMAT 'Gzzzzzzzzz') IC_prev_Month   , CAST(SUM(
CASE	
	WHEN LogMonth = (CAST(DATE'2024-07-31' AS DATE FORMAT'mmm-yyyy')(CHAR(8))) THEN  Impact_CPU 
	ELSE 0 
END)AS FORMAT 'Gzzzzzzzzz') IC_this_Month   , CAST(SUM(
CASE	
	WHEN LogMonth = (CAST((ADD_MONTHS(DATE'2024-07-31' - EXTRACT(DAY FROM DATE'2024-07-31') + 1,
		-2)) AS DATE FORMAT'mmm-yyyy')(CHAR(8))) THEN  QueryCount 
	ELSE 0 
END) AS FORMAT 'Gzzzzzzzzz') QC_prior_prev_Month   , CAST(SUM(
CASE	
	WHEN LogMonth = (CAST((ADD_MONTHS(DATE'2024-07-31' - EXTRACT(DAY FROM DATE'2024-07-31') + 1,
		-1)) AS DATE FORMAT'mmm-yyyy')(CHAR(8))) THEN  QueryCount 
	ELSE 0 
END) AS FORMAT 'Gzzzzzzzzz') QC_prev_Month   , CAST(SUM(
CASE	
	WHEN LogMonth = (CAST(DATE'2024-07-31' AS DATE FORMAT'mmm-yyyy')(CHAR(8))) THEN  QueryCount 
	ELSE 0 
END) AS FORMAT 'Gzzzzzzzzz') QC_this_Month   
FROM(
SELECT	   CAST(LogDate AS DATE FORMAT'mmm-yyyy')(CHAR(8)) LogMonth,
		TopUser.USERNAME, SUM(Ampcputime + ParserCPUTime) AS TotalCPU,
		SUM(MaxAMPCPUTime * NumOfActiveAMPs)  AS Impact_CPU   , COUNT(1) QueryCount 
FROM	pdcrinfo.dbqlogtbl_hst lg  RIGHT OUTER JOIN(
SELECT	USERNAME 
FROM	   pdcrinfo.dbqlogtbl_hst 
WHERE	LogDate BETWEEN(DATE'2024-07-31' - EXTRACT(DAY FROM DATE'2024-07-31') + 1)   
	AND DATE'2024-07-31' 
QUALIFY	   ROW_NUMBER() OVER(
ORDER BY SUM(MaxAMPCPUTime * NumOfActiveAMPs) DESC) <= 20 
GROUP BY 1) TopUser   
	ON lg.USERNAME = TopUser.USERNAME 
WHERE	LogDate BETWEEN(ADD_MONTHS(DATE'2024-07-31' - EXTRACT(DAY FROM DATE'2024-07-31') + 1,
		-2)) 
	AND DATE'2024-07-31' 
	AND numsteps > 0 
	AND ampcputime > 0 
GROUP BY 1, 2) a 
GROUP BY 2