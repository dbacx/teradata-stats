-- =============================================================================
-- Component   : Missing at PARTITION level
-- =============================================================================
-- Description : Identifies PPI (Partitioned Primary Index) tables without PARTITION
--               column statistics. Uses DBC.IndexConstraints to detect PPI tables
--               and DBC.StatsV to check for missing PARTITION-level stats. Critical
--               for query optimization on partitioned tables. Excludes system databases.
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Modificado  : 2026-05-13 - Integración y optimización de motor SQL para Módulo 2
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Tablas_Con_Datos AS (
    -- Validamos espacio físico (Nota: Las tablas NOS no ocupan PermSpace, 
    -- por lo que esto las filtraría automáticamente si no usamos un LEFT JOIN)
    SELECT DatabaseName, TableName
    FROM DBC.TableSizeV
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 0 
),
Estadisticas_De_Particion AS (
    SELECT DatabaseName, TableName
    FROM DBC.StatsV
    WHERE UPPER(ColumnName) = 'PARTITION'
    GROUP BY 1, 2
)
SELECT DISTINCT 
    t.DatabaseName, 
    t.TableName,
    t.PartitioningLevels,
    'COLLECT STATISTICS COLUMN PARTITION ON ' || TRIM(t.DatabaseName) || '.' || TRIM(t.TableName) || ';' AS Action_SQL
FROM DBC.TablesV t
-- Cambiamos a LEFT JOIN si quieres ver tablas NOS (que tienen 0 PermSpace)
-- O mantenemos INNER JOIN si solo te interesan tablas físicas con datos
INNER JOIN Tablas_Con_Datos td
    ON t.DatabaseName = td.DatabaseName
    AND t.TableName = td.TableName
LEFT JOIN Estadisticas_De_Particion s 
    ON t.DatabaseName = s.DatabaseName 
    AND t.TableName = s.TableName 
WHERE t.PartitioningLevels > 0 
  AND s.TableName IS NULL      
  -- FILTRO CLAVE: Excluimos Foreign Tables (NOS)
  AND t.AuthName is null 
  AND t.TableKind IN ('T', 'O', 'Q') 
  AND t.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
ORDER BY t.DatabaseName, t.TableName;