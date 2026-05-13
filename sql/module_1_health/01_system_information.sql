-- =============================================================================
-- Component   : System Information
-- =============================================================================
-- Description : Obtiene informacion general de capacidad y version del sistema
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-12
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================
-- [1/2] Hardware: Nodes, AMPs, PEs, CPUs, Memory, Space, Gateways
exec pdcrinfo.system_config_rpt;

-- [2/2] Software: Version y Release
locking row for access
SELECT InfoKey AS Metrica, InfoData AS Valor
FROM DBC.DBCInfo
WHERE InfoKey IN ('VERSION')
ORDER BY InfoKey;