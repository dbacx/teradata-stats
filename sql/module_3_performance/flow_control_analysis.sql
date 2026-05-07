-- =============================================================================
-- Component   : Flow Control Analysis
-- =============================================================================
-- Description : Analyzes flow control metrics including flow control time,
--               count, and AWT saturation to classify system health status
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

CREATE	VOLATILE TABLE FlowControl, NO Log AS (
SELECT	Cal.Year_of_Calendar AS Year_of_Calendar, Cal.Month_of_year AS Month_of_year,
		Cal.Week_of_Year AS Week_of_Year, (dt1.LogDate - ((dt1.LogDate - DATE '0001-01-07') MOD 7)) AS BOW ,
		Cal.Day_of_Month AS Day_of_Month ,Cal.Day_of_Week AS Day_of_Week,
		dt1.LogDate AS LogDate,WorkPeriod AS WorkPeriod,PERIOD AS PERIOD,
		LogHour AS LogHour,NodeType AS NodeType
                            ,Max(dt1.MaxAWT) AS MaxAWT, Max(dt1.AvgAWT)AS AvgAWT,
		Max(dt1.MinAWT) AS MinAWT, Max(dt1.MaxAWTUtil) AS MaxAWTUtil,
		Avg(dt1.AvgAWTUtil) AS AvgAWTUtil
                            , Min(dt1.MinAWTUtil) AS MinAWTUtil,
		Max(dt1.MSGWORKNEW) AS MSGWORKNEW, Max(dt1.MSGWORKONE) AS MSGWORKONE,
		Max(dt1.MSGWORKTWO) AS MSGWORKTWO
                           , Max(dt1.MSGWORKTHREE)  AS MSGWORKTHREE,
		Max(dt1.WorkTypeInuse04) AS WorkTypeInuse04, Max(dt1.WorkTypeInuse05) AS WorkTypeInuse05,
		Max(dt1.WorkTypeInuse06) AS WorkTypeInuse06
                           , Max(dt1.WorkTypeInuse07) AS WorkTypeInuse07,
		Max(dt1.MSGWORKEXPNEW) AS MSGWORKEXPNEW, Max(dt1.MSGWORKEXPONE) AS MSGWORKEXPONE,
		Max(dt1.WorkTypeInuse10) AS WorkTypeInuse10
                            , Max(dt1.WorkTypeInuse11) AS WorkTypeInuse11,
		Max(dt1.MSGWORKSPAWN) AS MSGWORKSPAWN, Max(dt1.MSGWORKNORM) AS MSGWORKNORM,
		Max(dt1.MSGWORKABORT) AS MSGWORKABORT
                           , Max(dt1.MSGWORKCONTROL) AS MSGWORKCONTROL,
		Sum(dt1.AWT_0Pct)  AS AWT_0Pct, Sum(dt1.AWT1_15Pct)  AS AWT1_15Pct,
		Sum(dt1.AWT16_35Pct)  AS AWT16_35Pct, Sum(dt1.AWT36_50Pct) AS AWT36_50Pct,
		Sum(dt1.AWT51_65Pct) AS AWT51_65Pct, Sum(dt1.AWT66_85Pct)  AS AWT66_85Pct
                            , Sum(dt1.AWT86_99Pct) AS AWT86_99Pct,
		Sum(dt1.AWT_100Pct) AS AWT_100Pct, Sum(dt1.AWT_Allocation) AS AWT_Allocation,
		Max(AWTLIMIT) AS AWTLIMIT, Max(MsgQueDepth) AS MsgQueDepth
                           , Sum(InFlowControl) AS InFlowControl,
		Sum(FlowCtlCnt) AS FlowCtlCnt, Sum(FlowCtlTime) AS FlowCtlTime,
		Max(MaxFlowCtlTimePct) AS MaxFlowCtlTimePct, Max(AvgFlowCtlTimePct) AS AvgFlowCtlTimePct,
		Max(MaxInuseMax) AS InuseMax, Max(MaxInuseMax) AS MaxInuseMax,
		Avg(AvgInuseMax) AS AvgInuseMax
                            , Max(AWTINUSE) AS MaxAWTINUSE,
		Avg(AWTINUSE) AS AvgAWTINUSE, Min(Available) AS Available, Min(AvailableMin) AS AvailableMin,
		Sum(dt1.AVAIL_0Pct) AS AVAIL_0Pct, Sum(dt1.AVAIL1_15Pct) AS AVAIL1_15Pct,
		Sum(dt1.AVAIL16_35Pct) AS AVAIL16_35Pct
                           , Sum(dt1.AVAIL36_50Pct) AS AVAIL36_50Pct,
		Sum(dt1.AVAIL51_65Pct) AS AVAIL51_65Pct, Sum(dt1.AVAIL66_85Pct) AS AVAIL66_85Pct,
		Sum(dt1.AVAIL86_99Pct) AS AVAIL86_99Pct, Sum(dt1.AVAIL_100Pct) AS AVAIL_100Pct
                           
FROM(
SELECT	LogDate, WorkPeriod, PERIOD, LogHour, Log10Minute, NodeType,
		MaxAWT, AvgAWT, MinAWT, MaxAWTUtil, AvgAWTUtil, MinAWTUtil, MSGWORKNEW AS MSGWORKNEW,
		MSGWORKONE AS MSGWORKONE, MSGWORKTWO AS MSGWORKTWO, MSGWORKTHREE AS MSGWORKTHREE
                            , WorkTypeInuse04 AS WorkTypeInuse04,
		WorkTypeInuse05 AS WorkTypeInuse05, WorkTypeInuse06 AS WorkTypeInuse06,
		WorkTypeInuse07 AS WorkTypeInuse07, MSGWORKEXPNEW AS MSGWORKEXPNEW
                           , MSGWORKEXPONE AS MSGWORKEXPONE,
		WorkTypeInuse10 AS WorkTypeInuse10, WorkTypeInuse11 AS WorkTypeInuse11,
		MSGWORKABORT AS MSGWORKABORT, MSGWORKSPAWN AS MSGWORKSPAWN
                            , MSGWORKNORM AS MSGWORKNORM, MSGWORKCONTROL AS MSGWORKCONTROL,
		AWT_0Pct, AWT1_15Pct, AWT16_35Pct, AWT36_50Pct, AWT51_65Pct,
		AWT66_85Pct, AWT86_99Pct, AWT_100Pct, AWT_Allocation, AWTLimit,
		MsgQueDepth, InFlowControl, FlowCtlCnt
                            , FlowCtlTime, MaxFlowCtlTimePct,
		AvgFlowCtlTimePct, MaxInuseMax, AvgInuseMax, AWTINUSE, Available,
		AvailableMin, AVAIL_0Pct, AVAIL1_15Pct, AVAIL16_35Pct, AVAIL36_50Pct,
		AVAIL51_65Pct, AVAIL66_85Pct, AVAIL86_99Pct, AVAIL_100Pct 
FROM	PDCRINFO.AWTRpt_Hst  a 
WHERE	LogDate BETWEEN   '2024-07-01' and '2024-09-30') dt1 INNER JOIN PDCRINFO.CALENDAR  cal 
	ON dt1.logdate = cal.calendar_date 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11  )
WITH	DATA NO PRIMARY INDEX 
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
END	Month_of_Year,xFlowCtlTime,
CASE	
	WHEN xFlowCtlTime <=5.00  
	OR   xFlowCtlCnt<1000.00  THEN 'Healthy' 
	WHEN xFlowCtlTime BETWEEN 5.01 
	AND 180.00 
	OR  xFlowCtlCnt BETWEEN 1000.01 
	AND 3000.00 THEN 'Degraded' 
	ELSE 'Critical' 
END	AS AWT_Notation,'Flow Control' as Flow_Control 
FROM	(
SEL	Month_of_Year, Cast (Max(FlowCtlTime/1000) AS FLOAT) xFlowCtlTime,
		Cast(Max(FlowCtlCnt) AS FLOAT) xFlowCtlCnt 
FROM	FlowControl 
GROUP BY 1) x 
order by Month_of_Year;
drop	table FlowControl;