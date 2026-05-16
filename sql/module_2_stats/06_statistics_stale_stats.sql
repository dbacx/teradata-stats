-- =============================================================================
-- Component 06: Stale Statistics (Time-Based)
-- =============================================================================
-- Description : Identifies tables where ALL statistics are older than the
--               configured threshold ({stale_days_threshold}). Uses DBC.StatsV 
--               with a HAVING MAX() clause to ensure whole-table collects are 
--               only recommended when the entire table's dictionary is stale.
--               Upgraded to ignore system databases, empty/small tables (<10MB),
--               and incompatible table kinds (Views, Macros, NOS).
--               Threshold is injected dynamically by the Python framework.
-- 
-- Version     : 1.1.0
-- Date        : 2026-05-16
-- Modificado  : 2026-05-16 - Integración de blindaje físico (>10MB), exclusión
--                            de DBs de sistema, filtro TableKind (T,O,Q) y 
--                            corrección de lógica de agregación (HAVING MAX).
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Tablas_Fisicas AS (
    -- Blindaje: Omitimos tablas vacías o solo con headers (>10MB)
    SELECT DatabaseName, TableName
    FROM DBC.TableSizeV
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 10485760 
)
SELECT 
    s.DatabaseName, 
    s.TableName,
    MAX(CAST(s.LastCollectTimeStamp AS DATE)) AS Last_Collect_Date,
    'COLLECT STATISTICS ON ' || TRIM(s.DatabaseName) || '.' || TRIM(s.TableName) || ';' AS Action_SQL
FROM DBC.StatsV s
INNER JOIN DBC.TablesV t
    ON s.DatabaseName = t.DatabaseName
    AND s.TableName = t.TableName
INNER JOIN Tablas_Fisicas tf
    ON s.DatabaseName = tf.DatabaseName
    AND s.TableName = tf.TableName
WHERE t.TableKind IN ('T', 'O', 'Q')
  AND s.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
  AND t.AuthName is null
GROUP BY 1, 2
-- Corrección matemática: Garantiza que la estadística MÁS RECIENTE de la tabla 
-- siga siendo más vieja que tu umbral de inyección de Python.
HAVING MAX(CAST(s.LastCollectTimeStamp AS DATE)) < CURRENT_DATE - {stale_days_threshold}
ORDER BY Last_Collect_Date ASC;