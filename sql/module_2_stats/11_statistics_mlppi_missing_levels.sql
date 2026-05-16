-- =============================================================================
-- Component 11: MLPPI Missing Partition Levels
-- =============================================================================
-- Description : Identifies Multi-Level Partitioned Primary Index (MLPPI) tables
--               (PartitioningLevels > 1) lacking compound statistics on the 
--               PARTITION keyword. Uses DBC.TablesV to detect MLPPI structures, 
--               DBC.StatsV to verify combined partition statistics presence, 
--               and DBC.TableSizeV to exclude empty tables and table headers. 
--               Critical for enabling proper Partition Elimination across all 
--               levels. Excludes system databases and NOS foreign tables.
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-15
-- Modificado  : 2026-05-15 - Creación de validación avanzada de arquitectura MLPPI
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================


WITH Tablas_Vacias AS (
    -- La única forma infalible de detectar tablas vacías: leer la última recolección
    SELECT DatabaseName, TableName
    FROM DBC.StatsV
    WHERE StatsId <> 0
    GROUP BY 1, 2
    HAVING MAX(RowCount) > 0
),
Tablas_Con_Datos AS (
    -- Excluimos vacías confirmadas y elevamos el umbral físico para ignorar los Table Headers
    SELECT ts.DatabaseName, ts.TableName
    FROM DBC.TableSizeV ts
    LEFT JOIN Tablas_Vacias tv
        ON ts.DatabaseName = tv.DatabaseName
       AND ts.TableName = tv.TableName
    WHERE tv.TableName IS NULL
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 10485760 -- > 10 MB
),
Estadisticas_MLPPI AS (
    -- Validamos la buena práctica: Estadísticas compuestas que incluyan PARTITION
    SELECT DatabaseName, TableName
    FROM DBC.StatsV
    WHERE UPPER(ColumnName) LIKE '%PARTITION%' 
      AND ExpressionCount > 1
    GROUP BY 1, 2
)
SELECT DISTINCT 
    t.DatabaseName, 
    t.TableName,
    t.PartitioningLevels,
    '-- REQUIERE REVISIÓN: Tabla MLPPI requiere COLLECT STATISTICS COLUMN (Col_Particion, PARTITION);' AS Action_SQL
FROM DBC.TablesV t
INNER JOIN Tablas_Con_Datos td
    ON t.DatabaseName = td.DatabaseName
    AND t.TableName = td.TableName
LEFT JOIN Estadisticas_MLPPI s 
    ON t.DatabaseName = s.DatabaseName 
    AND t.TableName = s.TableName 
WHERE t.PartitioningLevels > 1 
  AND s.TableName IS NULL      -- Carecen de estadísticas combinadas (Columna, PARTITION)
  AND t.TableKind IN ('T', 'O', 'Q') 
  AND t.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
  AND t.AuthName is null
ORDER BY t.DatabaseName, t.TableName;