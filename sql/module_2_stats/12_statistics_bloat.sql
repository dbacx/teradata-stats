-- =============================================================================
-- Component   : Stats Bloat (Demasiadas Estadísticas)
-- =============================================================================
-- Description : Identifica tablas con un exceso de estadísticas recolectadas
--               (> 15 por defecto). Genera el comando DROP STATISTICS a nivel
--               de tabla (ADVERTENCIA: Borra TODAS las estadísticas de la tabla).
-- 
-- Date        : 2026-05-15
-- Version     : 1.1.0
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================
-- 
-- Version     : 1.1.0
-- Date        : 2026-05-15
-- Modificado  : 2026-05-15 - Ajuste TableKind (T,O,Q) y blindaje contra Table Headers
-- Author      : Ricardo Enciso
-- Environment : Teradata 20

WITH Tablas_Con_Datos AS (
    SELECT DatabaseName, TableName, SUM(CurrentPerm) AS CurrentPerm
    FROM DBC.TableSizeV
    GROUP BY 1, 2
    -- Blindaje físico: ignorar cascarones vacíos y Table Headers (< 10 MB)
    HAVING SUM(CurrentPerm) > 10485760 
)
SELECT 
    s.DatabaseName, 
    s.TableName, 
    COUNT(DISTINCT s.StatsId) AS Total_Estadisticas,
    CAST(MAX(td.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    -- Genera el borrado total de estadísticas a nivel de tabla
    'DROP STATISTICS ON ' || TRIM(s.DatabaseName) || '.' || TRIM(s.TableName) || ';' AS Action_SQL
FROM DBC.StatsV s
INNER JOIN DBC.TablesV t 
    ON s.DatabaseName = t.DatabaseName 
    AND s.TableName = t.TableName
INNER JOIN Tablas_Con_Datos td
    ON s.DatabaseName = td.DatabaseName
    AND s.TableName = td.TableName
WHERE t.TableKind IN ('T', 'O', 'Q')
  AND s.StatsId <> 0 
  AND s.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
  AND t.AuthName is null
GROUP BY 1, 2
HAVING Total_Estadisticas > 15 -- Umbral de arquitectura (Ajustable)
ORDER BY Total_Estadisticas DESC, Size_GB DESC;