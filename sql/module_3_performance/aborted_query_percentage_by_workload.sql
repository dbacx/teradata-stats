-- =============================================================================
-- Component   : Aborted Query Percentage by Workload
-- =============================================================================
-- Description : Analyzes aborted queries by workload, categorizing aborts
--               as TASM Aborted or User Aborted, and calculates total CPU impact
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT	 WDName, SUM(AMPCPUTime) as TotalCPU , count (1) as AbortCount,
		CASE   
	WHEN           errorcode IN ( 3149, 3150, 3151, 3152, 3153, 3154,
		3155, 3156,3162 , 3163, 3298) THEN 'TASM Aborted'  
	WHEN           errorcode IN(2646, 3134, 3110) THEN 'User Aborted' 
END	  AbortType, Errortext, errorcode, logdate 
FROM	          pdcrinfo.dbqlogtbl_hst  
WHERE(logdate BETWEEN DATE'2024-07-01'  
	AND   DATE'2024-07-31')  
	AND AbortType in ('TASM Aborted', 'User Aborted') 
	AND AbortFlag = 'T' 
GROUP BY 1,4,5,6,7 
ORDER BY 2 DESC;