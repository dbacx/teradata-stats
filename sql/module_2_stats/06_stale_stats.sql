-- =============================================================================
-- Component   : Stale Statistics
-- =============================================================================
-- Description : Identifies statistics with collection timestamps older than the
--               configured threshold (placeholder {stale_days_threshold}). Uses
--               DBC.StatsV to detect stale statistics that need refresh. Critical
--               for maintaining accurate optimizer statistics. Threshold is injected
--               dynamically by the Python framework.
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Modificado  : 2026-05-13 - Integración y optimización de motor SQL para Módulo 2
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT 
    src.DatabaseName                                        AS DatabaseName,
    src.TableName                                           AS TableName,
    'TABLE LEVEL'                                           AS ObjectName,
    'Stale Statistics'                                      AS FindingCategory,
    CAST(src.Last_Collect_Date AS TIMESTAMP(0))             AS LastCollectTimeStamp,
    'COLLECT STATISTICS ' || TRIM(src.DatabaseName) || '.' || TRIM(src.TableName) || ';' AS RemediationDDL
FROM (
    SELECT DISTINCT 
        DatabaseName, 
        TableName,
        MAX(CAST(LastCollectTimeStamp AS DATE)) AS Last_Collect_Date
    FROM DBC.StatsV
    WHERE CAST(LastCollectTimeStamp AS DATE) < CURRENT_DATE - {stale_days_threshold}
    GROUP BY 1, 2
) src
ORDER BY src.Last_Collect_Date ASC;
