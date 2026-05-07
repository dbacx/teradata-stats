-- =============================================================================
-- Component   : Monthly CPU Utilization (Weekday)
-- =============================================================================
-- Description : Executes stored procedure to report CPU utilization metrics
--               for weekday analysis over a specified date range
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

execute pdcrinfo.CapSystem_CPU_Rpt(DATE'2024-07-01' ,DATE'2024-07-31')