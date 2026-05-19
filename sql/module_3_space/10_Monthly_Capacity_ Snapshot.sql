-- =============================================================================
-- Component   3.10: Monthly Capacity Snapshot (Last Day of Month)
-- =============================================================================
-- Description : Extracts storage capacity (Spool, Temp, Perm, PeakPerm) strictly
--               as a snapshot on the last day of each month. 
--               Optimized using LAST_DAY() native function to avoid scanning 
--               intermediate days. Excludes system databases for Perm calculations.
--               Safe CAST applied to prevent Numeric Overflow (2616).
-- Version     : 1.1.0
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Month_End_Dates AS (
    -- Filtramos únicamente el último día de cada mes (ej. últimos 12 meses)
    -- Usamos la función nativa LAST_DAY() para compatibilidad universal
    SELECT calendar_date AS LogDate
    FROM SYS_CALENDAR.CALENDAR
    WHERE calendar_date BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE
      AND calendar_date = LAST_DAY(calendar_date)
),
Spool_Snapshot AS (
    -- Foto exacta del Spool y Temp al cierre de mes
    SELECT 
        s.LogDate,
        AVG(CAST(s.MaxSpool AS FLOAT)) AS avg_max_spool,
        AVG(CAST(s.MaxTemp AS FLOAT)) AS avg_max_temp
    FROM PDCRINFO.SpoolSpace_hst s
    INNER JOIN Month_End_Dates m 
        ON s.LogDate = m.LogDate
    GROUP BY 1
),
Perm_Snapshot AS (
    -- Foto exacta del Perm al cierre de mes (Excluyendo DBs de sistema)
    SELECT 
        p.LogDate,
        SUM(CAST(p.CurrentPerm AS FLOAT)) AS sum_currperm,
        SUM(CAST(p.PeakPerm AS FLOAT)) AS sum_peakperm
    FROM PDCRINFO.DatabaseSpace_Hst p
    INNER JOIN Month_End_Dates m 
        ON p.LogDate = m.LogDate
    WHERE UPPER(p.DatabaseName) NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA',
        'CONSOLE','MLDB','PDCRADMIN','SYSUDTLIB','TD_SYSFNLIB',
        'TDAAS_DB','BARADMIN','BARUSERS','CRASHDUMPS','EXTERNAL_AP',
        'LAB1CANALES','LABS_GROUP_LAB','LOCKLOGSHREDDER','PDCRADM',
        'PDCRINFO','SYSJDBC','SYSUIF','TD_SYSGPL','TD_SYSXML'
    )
    GROUP BY 1
)
SELECT 
    COALESCE(p.LogDate, s.LogDate) AS Mes_Snapshot,
    CAST(COALESCE(s.avg_max_spool, 0) / 1099511627776.0 AS DECIMAL(18,4)) AS Spool_TB,
    CAST(COALESCE(s.avg_max_temp, 0) / 1099511627776.0 AS DECIMAL(18,4)) AS Temp_TB,
    CAST(COALESCE(p.sum_currperm, 0) / 1099511627776.0 AS DECIMAL(18,4)) AS CurrentPerm_TB,
    CAST(COALESCE(p.sum_peakperm, 0) / 1099511627776.0 AS DECIMAL(18,4)) AS PeakPerm_TB
FROM Perm_Snapshot p
FULL OUTER JOIN Spool_Snapshot s 
    ON p.LogDate = s.LogDate
ORDER BY Mes_Snapshot ASC;