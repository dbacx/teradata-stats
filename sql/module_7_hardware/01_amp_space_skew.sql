-- =============================================================================
-- Component   : AMP Space Skew
-- =============================================================================
-- Description : Calculates total space per AMP to detect disk-level skew
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    Vproc AS AMP_ID,
    SUM(CurrentPerm) AS TotalSpace_Bytes
FROM DBC.TableSizeV
GROUP BY 1
ORDER BY 2 DESC;
