-- =============================================================================
-- Component   : Monthly Query Skew
-- =============================================================================
-- Description : Analyzes query skew by calculating total CPU and impact CPU
--               to identify skewed queries affecting system performance
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

sel	cast(starttime as date ) as logdate , sum(AMPCPUtime + ParserCPUTime )as Totl_CPU ,
		sum(MAXAMPCPUTime*Numofactiveamps) As Impct_CPU 
from	 pdcrinfo.dbqlogtbl_hst   
where	logdate between  DATE'2024-07-01'  
	and   DATE'2024-07-31'  
group by 1;