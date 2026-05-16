-- =============================================================================
-- Component 3.01: Database Space Utilization (AMP-Aware)
-- =============================================================================
-- Description : Calculates database space utilization considering AMP Skew.
--               Uses MAX(CurrentPerm) * TotalAMPs to define the "Effective" 
--               space usage, which is the true bottleneck in a Teradata MPP.
-- Version     : 1.0.0
-- Author      : Ricardo Enciso
-- =============================================================================

SELECT 
    DatabaseName,
    SUM(MaxPerm) / (1024.0**3) AS MaxPerm_GB,
    SUM(CurrentPerm) / (1024.0**3) AS CurrentPerm_GB,
    -- Effective Space: El AMP más lleno define el límite real de la base de datos
    (MAX(CurrentPerm) * (HASHAMP()+1)) / (1024.0**3) AS Effective_Space_GB,
    CAST((SUM(CurrentPerm) / NULLIF(SUM(MaxPerm), 0)) * 100 AS DECIMAL(5,2)) AS Global_Util_Pct,
    CAST(((MAX(CurrentPerm) * (HASHAMP()+1)) / NULLIF(SUM(MaxPerm), 0)) * 100 AS DECIMAL(5,2)) AS Effective_Util_Pct,
    -- Varianza de Skew: Si es > 10%, la BD está en riesgo por mala distribución
    CAST(((MAX(CurrentPerm) - AVG(CurrentPerm)) / NULLIF(AVG(CurrentPerm), 0)) * 100 AS DECIMAL(5,2)) AS Skew_Pct
FROM DBC.DiskSpaceV
WHERE DatabaseName NOT IN (
    'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT','TD_SERVER_DB',
    'VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM','LOCKLOGLSHREDDER','SQLJ',
    'SYSBAR','SYSADMIN','SYS_CALENDAR','TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA',
    'PDCRSTG','SYSDBA', 'CONSOLE'
)
GROUP BY 1
HAVING SUM(MaxPerm) > 0
ORDER BY Effective_Util_Pct DESC;