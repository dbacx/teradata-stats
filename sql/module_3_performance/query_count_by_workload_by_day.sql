-- =============================================================================
-- Component   : Query Count by Workload by Day
-- =============================================================================
-- Description : Executes stored procedure to report query count metrics
--               by workload on a daily basis for performance analysis
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

execute pdcrinfo.CapTASMWkld_Rpt(DATE'2024-07-01' ,DATE'2024-07-31'); 