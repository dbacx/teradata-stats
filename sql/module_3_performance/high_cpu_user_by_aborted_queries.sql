-- =============================================================================
-- Component   : High CPU User by Aborted Queries
-- =============================================================================
-- Description : Identifies users with high CPU consumption from aborted queries
--               to pinpoint problematic users and query patterns
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT	 logdate,Errorcode,errortext,wdname,username,statementtype,
		AppID, Sum(AMPCPUTime) AS TotalCPU ,Count(QueryID) AS TotalQrys,
		errortext AS AbortedBy, Logdate 
FROM	pdcrinfo.DBQLogTbl_Hst 
WHERE	LogDate  BETWEEN '2024-07-01'   
	AND   '2024-07-31'   
	AND	  AbortFlag = 'T'  
	AND ERRORCODE IN ( 2938, 3134, 3156, 3917,2646,2631,3514)	 
GROUP BY 1,2,3,4,5,6,7 
ORDER BY 1,2,3,4,5,6,7;