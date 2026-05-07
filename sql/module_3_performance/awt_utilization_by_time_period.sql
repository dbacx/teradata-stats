-- =============================================================================
-- Component   : AWT Utilization by Time Period
-- =============================================================================
-- Description : Executes stored procedure to report AWT (Active Workload Throttling) 
--               utilization metrics for a specified date range
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

Execute pdcrinfo.CapAWTSum_Rpt(DATE'2024-07-01', DATE'2024-07-31'); 