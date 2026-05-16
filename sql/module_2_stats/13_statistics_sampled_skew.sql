-- =============================================================================
-- Component 13: Sampled Skew (Anti-Patrón de Muestreo)
-- =============================================================================
-- Description : Identifies low-cardinality/skewed columns (UniqueValueCount < 100)
--               that are incorrectly using sampled statistics (SampleSizePct > 0).
--               According to Teradata best practices, skewed columns must be 
--               collected at 100% to avoid inaccurate histograms and poor optimizer 
--               plans. Uses DBC.StatsV and DBC.TableSizeV. Generates a 'USING NO 
--               SAMPLE' command to override dictionary memory and force a full scan.
--               Excludes system databases.
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-15
-- Modificado  : 2026-05-15 - Creación de validación avanzada para corregir sesgo
--               y ajuste de sentencia a USING NO SAMPLE.
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Tablas_Con_Datos AS (
    SELECT DatabaseName, TableName
    FROM DBC.TableSizeV
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 10485760 -- Blindaje físico contra vacías/headers
)
SELECT 
    s.DatabaseName, 
    s.TableName,
    s.ColumnName,
    s.RowCount AS Filas_Tabla,
    s.UniqueValueCount AS Valores_Unicos,
    s.SampleSizePct,
    -- Generación de la contraorden explícita para borrar la memoria del muestreo
    'COLLECT STATISTICS USING NO SAMPLE COLUMN (' || COALESCE(TRIM(s.ColumnName), 'N/A') || ') ON ' || TRIM(s.DatabaseName) || '.' || TRIM(s.TableName) || ';' AS Action_SQL
FROM DBC.StatsV s
INNER JOIN DBC.TablesV t 
    ON s.DatabaseName = t.DatabaseName 
    AND s.TableName = t.TableName
INNER JOIN Tablas_Con_Datos td
    ON s.DatabaseName = td.DatabaseName
    AND s.TableName = td.TableName
WHERE t.TableKind IN ('T', 'O', 'Q')
  AND s.StatsId <> 0 
  AND s.RowCount > 100000 
  AND COALESCE(UPPER(s.ColumnName), '') <> 'PARTITION' 
  -- Regla estricta: Muestreo activo pero con bajísima cardinalidad
  AND s.SampleSizePct > 0 AND s.SampleSizePct < 100
  AND s.UniqueValueCount < 100 
  AND s.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
  AND t.AuthName is null
ORDER BY s.RowCount DESC;