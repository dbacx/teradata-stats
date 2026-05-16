-- =============================================================================
-- Component   : AWT Analysis
-- =============================================================================
-- Description : Analyzes AWT (Active Workload Throttling) saturation metrics,
--               including percentage of AMPs crossing 85% threshold and
--               monthly AWT utilization patterns
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
		LogHour AS LogHour,NodeType AS NodeType , Max(dt1.MaxAWT) AS MaxAWT,
		Max(dt1.AvgAWT)AS AvgAWT, Max(dt1.MinAWT) AS MinAWT, Max(dt1.MaxAWTUtil) AS MaxAWTUtil,
		Avg(dt1.AvgAWTUtil) AS AvgAWTUtil , Min(dt1.MinAWTUtil) AS MinAWTUtil,
		Max(dt1.MSGWORKNEW) AS MSGWORKNEW, Max(dt1.MSGWORKONE) AS MSGWORKONE,
		Max(dt1.MSGWORKTWO) AS MSGWORKTWO , Max(dt1.MSGWORKTHREE)  AS MSGWORKTHREE,
		Max(dt1.WorkTypeInuse04) AS WorkTypeInuse04, Max(dt1.WorkTypeInuse05) AS WorkTypeInuse05,
		Max(dt1.WorkTypeInuse06) AS WorkTypeInuse06 , Max(dt1.WorkTypeInuse07) AS WorkTypeInuse07,
		Max(dt1.MSGWORKEXPNEW) AS MSGWORKEXPNEW, Max(dt1.MSGWORKEXPONE) AS MSGWORKEXPONE,
		Max(dt1.WorkTypeInuse10) AS WorkTypeInuse10 , Max(dt1.WorkTypeInuse11) AS WorkTypeInuse11,
		Max(dt1.MSGWORKSPAWN) AS MSGWORKSPAWN, Max(dt1.MSGWORKNORM) AS MSGWORKNORM,
		Max(dt1.MSGWORKABORT) AS MSGWORKABORT , Max(dt1.MSGWORKCONTROL) AS MSGWORKCONTROL,
		Sum(dt1.AWT_0Pct)  AS AWT_0Pct, Sum(dt1.AWT1_15Pct)  AS AWT1_15Pct,
		Sum(dt1.AWT16_35Pct)  AS AWT16_35Pct, Sum(dt1.AWT36_50Pct) AS AWT36_50Pct,
		Sum(dt1.AWT51_65Pct) AS AWT51_65Pct, Sum(dt1.AWT66_85Pct)  AS AWT66_85Pct ,
		Sum(dt1.AWT86_99Pct) AS AWT86_99Pct, Sum(dt1.AWT_100Pct) AS AWT_100Pct,
		Sum(dt1.AWT_Allocation) AS AWT_Allocation, Max(AWTLIMIT) AS AWTLIMIT,
		Max(MsgQueDepth) AS MsgQueDepth , Sum(InFlowControl) AS InFlowControl,
		Sum(FlowCtlCnt) AS FlowCtlCnt, Sum(FlowCtlTime) AS FlowCtlTime,
		Max(MaxFlowCtlTimePct) AS MaxFlowCtlTimePct, Max(AvgFlowCtlTimePct) AS AvgFlowCtlTimePct,
		Max(MaxInuseMax) AS InuseMax, Max(MaxInuseMax) AS MaxInuseMax,
		Avg(AvgInuseMax) AS AvgInuseMax , Max(AWTINUSE) AS MaxAWTINUSE,
		Avg(AWTINUSE) AS AvgAWTINUSE, Min(Available) AS Available, Min(AvailableMin) AS AvailableMin,
		Sum(dt1.AVAIL_0Pct) AS AVAIL_0Pct, Sum(dt1.AVAIL1_15Pct) AS AVAIL1_15Pct,
		Sum(dt1.AVAIL16_35Pct) AS AVAIL16_35Pct , Sum(dt1.AVAIL36_50Pct) AS AVAIL36_50Pct,
		Sum(dt1.AVAIL51_65Pct) AS AVAIL51_65Pct, Sum(dt1.AVAIL66_85Pct) AS AVAIL66_85Pct,
		Sum(dt1.AVAIL86_99Pct) AS AVAIL86_99Pct, Sum(dt1.AVAIL_100Pct) AS AVAIL_100Pct   
FROM(
SELECT	LogDate, WorkPeriod, PERIOD, LogHour, Log10Minute, NodeType,
		MaxAWT, AvgAWT, MinAWT, MaxAWTUtil, AvgAWTUtil, MinAWTUtil, MSGWORKNEW AS MSGWORKNEW,
		MSGWORKONE AS MSGWORKONE, MSGWORKTWO AS MSGWORKTWO, MSGWORKTHREE AS MSGWORKTHREE ,
		WorkTypeInuse04 AS WorkTypeInuse04, WorkTypeInuse05 AS WorkTypeInuse05,
		WorkTypeInuse06 AS WorkTypeInuse06, WorkTypeInuse07 AS WorkTypeInuse07,
		MSGWORKEXPNEW AS MSGWORKEXPNEW , MSGWORKEXPONE AS MSGWORKEXPONE,
		WorkTypeInuse10 AS WorkTypeInuse10, WorkTypeInuse11 AS WorkTypeInuse11,
		MSGWORKABORT AS MSGWORKABORT, MSGWORKSPAWN AS MSGWORKSPAWN ,
		MSGWORKNORM AS MSGWORKNORM, MSGWORKCONTROL AS MSGWORKCONTROL,
		AWT_0Pct, AWT1_15Pct, AWT16_35Pct, AWT36_50Pct, AWT51_65Pct,
		AWT66_85Pct, AWT86_99Pct, AWT_100Pct, AWT_Allocation, AWTLimit,
		MsgQueDepth, InFlowControl, FlowCtlCnt , FlowCtlTime, MaxFlowCtlTimePct,
		AvgFlowCtlTimePct, MaxInuseMax, AvgInuseMax, AWTINUSE, Available,
		AvailableMin, AVAIL_0Pct, AVAIL1_15Pct, AVAIL16_35Pct, AVAIL36_50Pct,
		AVAIL51_65Pct, AVAIL66_85Pct, AVAIL86_99Pct, AVAIL_100Pct 
FROM	PDCRINFO.AWTRpt_Hst  a 
WHERE	LogDate BETWEEN DATE'2024-08-01'  
	and  DATE'2024-09-30') dt1 INNER JOIN PDCRINFO.CALENDAR  cal 
	ON dt1.logdate = cal.calendar_date 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11  )
WITH	DATA NO PRIMARY INDEX 
	ON 
COMMIT	PRESERVE ROWS;

Select distinct Month_Of_Year,Percentage_days_crossed_85_Per,Max_of_percentage_amp_qty from (Select distinct TT1.LogDate,TT1.Month_of_Year,Total_AMP_QTY,AWT_saturation_GT_85_in_AMPs,
cast((cast(AWT_saturation_GT_85_in_AMPs as float)/cast(Total_AMP_QTY as float))*100 as decimal(6,2)) as Percentage_of_AMP_QTY_GT_85 , 
Number_of_days_per_workperiod_AWT_GT_85,Report_Days,Workperiod_by_day_count, cast(Number_of_days_per_workperiod_AWT_GT_85 as float)/cast(Workperiod_by_day_count as float) *100 as Percentage_days_crossed_85_Per,
Max_of_percentage_amp_qty from (Select LogDate,WorkPeriod,Month_of_Year, (AWT_0Pct+AWT1_15Pct+AWT16_35Pct+AWT36_50Pct+AWT51_65Pct+AWT66_85Pct+AWT86_99Pct+AWT_100Pct) 
as Total_AMP_QTY from ( select LogDate,WorkPeriod,CASE Month_of_Year 
WHEN 1 THEN 'January'  WHEN 2 THEN 'February'  WHEN 3 THEN 'March'  WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' 
WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November'  WHEN 12 THEN 'December' 
END Month_of_Year, sum(AWT_0Pct) as AWT_0Pct, sum(AWT1_15Pct) as AWT1_15Pct, sum(AWT16_35Pct)AWT16_35Pct, sum(AWT36_50Pct)AWT36_50Pct,
sum(AWT51_65Pct)AWT51_65Pct, sum(AWT66_85Pct)AWT66_85Pct, sum(AWT86_99Pct)AWT86_99Pct, sum(AWT_100Pct)AWT_100Pct 
from FlowControl group by 1,2,3  )c )TT1, (Select LogDate,WorkPeriod, (AWT86_99Pct+AWT_100Pct) 
as AWT_saturation_GT_85_in_AMPs from ( select LogDate,WorkPeriod,CASE Month_of_Year 
WHEN 1 THEN 'January'  WHEN 2 THEN 'February'  WHEN 3 THEN 'March'  WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' 
WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November'  WHEN 12 THEN 'December' 
END Month_of_Year,sum(AWT86_99Pct)AWT86_99Pct, sum(AWT_100Pct)AWT_100Pct from FlowControl group by 1,2,3  )c )TT2, (select count(distinct LogDate) Report_Days,Report_Days*4 Workperiod_by_day_count from FlowControl)TT3,
(Select count(AWT_saturation_GT_85_in_AMPs) as Number_of_days_per_workperiod_AWT_GT_85 from ( Select LogDate,WorkPeriod, (AWT86_99Pct+AWT_100Pct) 
as AWT_saturation_GT_85_in_AMPs from ( select LogDate,WorkPeriod,CASE Month_of_Year 
WHEN 1 THEN 'January'  WHEN 2 THEN 'February'  WHEN 3 THEN 'March'  WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' 
WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November'  WHEN 12 THEN 'December' 
END Month_of_Year,sum(AWT86_99Pct)AWT86_99Pct, sum(AWT_100Pct)AWT_100Pct 
from FlowControl group by 1,2,3  )c  where  AWT_saturation_GT_85_in_AMPs>0)t) TT4,(Select max(Percentage_of_AMP_QTY_GT_85) as Max_of_percentage_amp_qty from (
Select distinct TT1.LogDate,TT1.WorkPeriod,TT1.Month_of_Year,Total_AMP_QTY,AWT_saturation_GT_85_in_AMPs,
cast((cast(AWT_saturation_GT_85_in_AMPs as float)/cast(Total_AMP_QTY as float))*100 as decimal(6,2)) as Percentage_of_AMP_QTY_GT_85 , 
Report_Days,Workperiod_by_day_count from (Select LogDate,WorkPeriod,Month_of_Year, (AWT_0Pct+AWT1_15Pct+AWT16_35Pct+AWT36_50Pct+AWT51_65Pct+AWT66_85Pct+AWT86_99Pct+AWT_100Pct) 
as Total_AMP_QTY from ( select LogDate,WorkPeriod,CASE Month_of_Year 
WHEN 1 THEN 'January'  WHEN 2 THEN 'February'  WHEN 3 THEN 'March'  WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' 
WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November'  WHEN 12 THEN 'December' 
END Month_of_Year, sum(AWT_0Pct) as AWT_0Pct, sum(AWT1_15Pct) as AWT1_15Pct, sum(AWT16_35Pct)AWT16_35Pct, sum(AWT36_50Pct)AWT36_50Pct,
sum(AWT51_65Pct)AWT51_65Pct, sum(AWT66_85Pct)AWT66_85Pct, sum(AWT86_99Pct)AWT86_99Pct, sum(AWT_100Pct)AWT_100Pct 
from FlowControl group by 1,2,3  )c )TT1, (Select LogDate,WorkPeriod, (AWT86_99Pct+AWT_100Pct) 
as AWT_saturation_GT_85_in_AMPs from ( select LogDate,WorkPeriod,CASE Month_of_Year 
WHEN 1 THEN 'January'  WHEN 2 THEN 'February'  WHEN 3 THEN 'March'  WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' 
WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November'  WHEN 12 THEN 'December' 
END Month_of_Year,sum(AWT86_99Pct)AWT86_99Pct, sum(AWT_100Pct)AWT_100Pct from FlowControl group by 1,2,3  )c )TT2, (select count(distinct LogDate) Report_Days,Report_Days*4 Workperiod_by_day_count from FlowControl)TT3) VV2 )TT5)FF;

drop table FlowControl;