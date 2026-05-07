-- =============================================================================
-- Component   : Baseline and Growth Trends
-- =============================================================================
-- Description : Compares monthly baseline metrics (active users, tables, space,
--               CPU, queries) between current and previous month to calculate
--               growth trends and identify capacity planning needs
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT	 aa.Parameter_P  Parameter_P ,bb.Value_P AS July_23,aa.Value_P AS Aug_23,
		((Aug_23-July_23)/NullIf(July_23,0))*100 (DECIMAL(18, 2)) Per_Growth_Last_Month 
FROM(
SELECT	     '1. Active Users'(VARCHAR(33))  Parameter_P, Cast(Count(DISTINCT username) AS DECIMAL(18,
		2)) Value_P  
FROM	    dbc.logonoff   
WHERE	     logdate BETWEEN Add_Months(DATE - Extract(DAY From DATE) + 1,
		-1) 
	AND Add_Months(DATE - Extract(DAY From DATE), 0) 
GROUP BY 1 
HAVING	   Parameter_P IS NOT NULL 
UNION	ALL 
SELECT	  '2. Tables' Parameter_P, Cast(Count(1)AS DECIMAL(18, 2)) Value_P 
FROM	    dbc.tablesv 
WHERE	    tablekind IN('T', 'O') 
	AND CreateTimeStamp BETWEEN    Add_Months(DATE - Extract(DAY From DATE) + 1,
		-1)  
	AND Add_Months(DATE - Extract(DAY From DATE), 0) 
UNION	 ALL   
SELECT	  '3. Space Consumed (TB)'  Parameter_P, Cast(Sum(CurrentPerm) / 1024 **4 AS DECIMAL(18,
		8)) Value_P 
FROM	   PDCRINFO.DatabaseSpace_Hst 
WHERE	     LogDate = Add_Months(DATE - Extract(DAY From DATE),
		0) 
UNION	ALL 
SELECT	  '4. Total_CPU For Month' Parameter_P, Cast(Sum(totaldailycpu) AS DECIMAL(18,
		2)) Value_P 
FROM(
SELECT	  Cast(Sum(AMPCPUTime + ParserCPUTime) AS DECIMAL(18, 2)) totaldailycpu  
FROM	      pdcrinfo.DBQLogTbl_hst 
WHERE	     logdate BETWEEN Add_Months(DATE - Extract(DAY From DATE) + 1,
		-1) 
	AND Add_Months(DATE - Extract(DAY From DATE), 0))a 
UNION	ALL 
SELECT	  '5. Average_CPU For Month' Parameter_P, Cast(Avg(totaldailycpu) AS  DECIMAL(18,
		2)) Value_P 
FROM(
SELECT	      logdate, Sum(AMPCPUTime + ParserCPUTime)totaldailycpu 
FROM	      pdcrinfo.DBQLogTbl_hst  
WHERE	    logdate BETWEEN Add_Months(DATE - Extract(DAY From DATE) + 1,
		-1)  
	AND Add_Months(DATE - Extract(DAY From DATE), 0)  
GROUP BY 1)b 
UNION	ALL 
SELECT	  '6. Total_Queries For Month' Parameter_P, Cast(Sum(totaldailycount) AS  DECIMAL(38,
		0)) Value_P 
FROM(
SELECT	    Logdate, Cast(Count(*) AS DECIMAL(38)) AS totaldailycount  
FROM	      pdcrinfo.DBQLogTbl_hst 
WHERE	    logdate BETWEEN Add_Months(DATE - Extract(DAY From DATE) + 1,
		-1)  
	AND Add_Months(DATE - Extract(DAY From DATE), 0) 
GROUP BY 1 
UNION	ALL 
SELECT	  logdate, Sum(b.querycount) AS QueryCount 
FROM	      pdcrinfo.DBQLSummaryTbl_Hst b  
WHERE	    b.logdate BETWEEN Add_Months(DATE - Extract(DAY From DATE) + 1,
		-1)  
	AND Add_Months(DATE - Extract(DAY From DATE), 0)
GROUP BY 1)c 
UNION	ALL  
SELECT	  '7. Average_Queries For Month' Parameter_P, Cast(Avg(totaldailycount)  AS  DECIMAL(38,
		0)) Value_P  
FROM(
SELECT	  logdate, Cast(Count(*) AS DECIMAL(38, 0)) AS totaldailycount  
FROM	    pdcrinfo.DBQLogTbl_hst 
WHERE	    logdate BETWEEN Add_Months(DATE - Extract(DAY From DATE) + 1,
		-1)  
	AND Add_Months(DATE - Extract(DAY From DATE), 0)   
GROUP BY 1 
UNION	ALL 
SELECT	 logdate, Sum(b.querycount) AS QueryCount  
FROM	      pdcrinfo.DBQLSummaryTbl_Hst b 
WHERE	     b.logdate BETWEEN Add_Months(DATE - Extract(DAY From DATE) + 1,
		-1)  
	AND Add_Months(DATE - Extract(DAY From DATE), 0)  
GROUP BY 1)d 
UNION	ALL  
SELECT	   '8. Complex_Queries For Month' Parameter_P, Cast(Count(1) AS  DECIMAL(18,
		2)) Value_P  
FROM(
SELECT	   logdate, queryid, Count(stepname) AS step 
FROM	         pdcrinfo.dbqlsteptbl_hst 
WHERE	    logdate BETWEEN Add_Months(DATE - Extract(DAY From DATE) + 1,
		-1) 
	AND Add_Months(DATE - Extract(DAY From DATE), 0) 
	AND stepname = 'JIN'  
GROUP BY logdate, queryid 
HAVING	   step > 5) e 
UNION	ALL  
SELECT	  '*. Number of Reports/Jobs', NULL Value_P 
FROM( 
SELECT	  1 dummy_table)a)aa, (
SELECT	     '1. Active Users'(VARCHAR(33))  Parameter_P, Cast(Count(DISTINCT username) AS  DECIMAL(18,
		2)) Value_P   
FROM	      dbc.logonoff 
WHERE	   logdate BETWEEN Trunc(Add_Months(Current_Date, -2), 'mm')  
	AND Last_Day(Add_Months(Current_Date, -2)) 
GROUP BY 1 
HAVING	   Parameter_P IS NOT NULL 
UNION	ALL  
SELECT	    '2. Tables' Parameter_P, Cast(Count(1) AS  DECIMAL(18,
		2)) Value_P 
FROM	    dbc.tablesv 
WHERE	    tablekind IN('T', 'O')  
	AND  CreateTimeStamp BETWEEN Trunc(Add_Months(Current_Date, -2),
		'mm')  
	AND Last_Day(Add_Months(Current_Date, -2)) 
UNION	ALL   
SELECT	  '3. Space Consumed (TB)'  Parameter_P, Cast(Sum(CurrentPerm) / 1024 **4 AS    DECIMAL(18,
		2)) Value_P 
FROM	    PDCRINFO.DatabaseSpace_Hst 
WHERE	    logdate = Add_Months(Add_Months(DATE, -1) - Extract(DAY From Add_Months(DATE,
		-1)), 0) 
UNION	ALL 
SELECT	 '4. Total_CPU For Month' Parameter_P, Cast(Sum(totaldailycpu) AS  DECIMAL(18,
		2)) Value_P 
FROM( 
SELECT	    Cast(Sum(AMPCPUTime + ParserCPUTime)  AS  DECIMAL(18,
		2))totaldailycpu  
FROM	    pdcrinfo.DBQLogTbl_hst  
WHERE	    logdate BETWEEN Trunc(Add_Months(Current_Date, -2), 'mm')  
	AND Last_Day(Add_Months(Current_Date, -2)))a 
UNION	ALL 
SELECT	 '5. Average_CPU For Month' Parameter_P, Cast(Avg(totaldailycpu) AS  DECIMAL(18,
		2)) Value_P 
FROM(
SELECT	    logdate, Cast(Sum(AMPCPUTime + ParserCPUTime) AS  DECIMAL(18,
		2))totaldailycpu  
FROM	   pdcrinfo.DBQLogTbl_hst 
WHERE	    logdate BETWEEN Trunc(Add_Months(Current_Date, -2), 'mm')  
	AND Last_Day(Add_Months(Current_Date, -2)) 
GROUP BY 1)b  
UNION	ALL  
SELECT	  '6. Total_Queries For Month' Parameter_P, Cast(Sum(totaldailycount) AS  DECIMAL(38)) Value_P 
FROM(
SELECT	   Logdate, Cast(Count(*) AS DECIMAL(38, 0)) AS totaldailycount  
FROM	     pdcrinfo.DBQLogTbl_hst  
WHERE	    logdate BETWEEN Trunc(Add_Months(Current_Date, -2), 'mm')  
	AND Last_Day(Add_Months(Current_Date, -2)) 
GROUP BY 1 
UNION	ALL 
SELECT	  logdate, Cast(Sum(b.querycount) AS  DECIMAL(18, 2)) AS QueryCount  
FROM	      pdcrinfo.DBQLSummaryTbl_Hst b  
WHERE	    b.logdate BETWEEN Trunc(Add_Months(Current_Date, -2),
		'mm')  
	AND Last_Day(Add_Months(Current_Date, -2)) 
GROUP BY 1)c  
UNION	ALL 
SELECT	   '7. Average_Queries For Month' Parameter_P, Cast(Avg(totaldailycount) AS  DECIMAL(18,
		2)) Value_P  
FROM( 
SELECT	  logdate, Cast(Count(*) AS DECIMAL(38, 0)) AS totaldailycount  
FROM	    pdcrinfo.DBQLogTbl_hst   
WHERE	    logdate BETWEEN Trunc(Add_Months(Current_Date, -2), 'mm')  
	AND Last_Day(Add_Months(Current_Date, -2)) 
GROUP BY 1 
UNION	ALL 
SELECT	    logdate, Cast(Sum(b.querycount) AS DECIMAL(38, 0)) QueryCount   
FROM	     pdcrinfo.DBQLSummaryTbl_Hst b 
WHERE	  b.logdate BETWEEN Trunc(Add_Months(Current_Date, -2), 'mm')  
	AND Last_Day(Add_Months(Current_Date, -2)) 
GROUP BY 1)d 
UNION	ALL  
SELECT	    '8. Complex_Queries For Month' Parameter_P, Cast(Count(1) AS  DECIMAL(18,
		2)) Value_P  
FROM(
SELECT	    logdate, queryid, Cast(Count(stepname) AS  DECIMAL(18,
		2)) AS step 
FROM	       pdcrinfo.dbqlsteptbl_hst 
WHERE	    logdate BETWEEN Trunc(Add_Months(Current_Date, -2), 'mm')  
	AND Last_Day(Add_Months(Current_Date, -2)) 
	AND stepname = 'JIN'  
GROUP BY logdate, queryid 
HAVING	   step > 5) e 
UNION	ALL 
SELECT	    '*. Number of Reports/Jobs', NULL Value_P 
FROM(
SELECT	   1 dummy_table)a)bb 
WHERE	    aa.Parameter_P = bb.Parameter_P 
ORDER BY 1;