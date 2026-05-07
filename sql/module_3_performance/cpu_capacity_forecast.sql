-- =============================================================================
-- Component   : CPU Capacity Forecast
-- =============================================================================
-- Description : Executes stored procedure to forecast CPU capacity trends
--               over a specified date range for capacity planning
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

execute pdcrinfo.CapSystem_CPU_Rpt (DATE'2023-08-08', DATE'2024-08-08');