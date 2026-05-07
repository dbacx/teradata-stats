-- =============================================================================
-- Component   : Delay Time Analysis
-- =============================================================================
-- Description : Analyzes query delay time metrics by month to identify
--               performance degradation and classify delay health status
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

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
                           END Month_of_Year, Max(delaytime) AS DelayTime, CASE WHEN Max(delaytime) < 60 THEN 'Healthy' WHEN Max(delaytime) BETWEEN 60 AND 600 THEN 'Degraded' ELSE 'Critical' END AS Delay,'Delay Time' as Delay_Time FROM pdcrinfo.dbqlogtbl_hst a INNER JOIN PDCRINFO.CALENDAR c ON a.logdate = c.calendar_date WHERE a.LogDate BETWEEN  '2024-07-01' and '2024-09-30' GROUP BY Month_of_Year order by Month_of_Year;