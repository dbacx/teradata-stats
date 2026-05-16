-- =============================================================================
-- Component   : Missing at Index Level
-- =============================================================================
-- Description : Identifies indexes (PI, SI, JI, PPI) without statistics. Uses
--               DBC.IndicesV to detect all index types and DBC.StatsV to check
--               for missing index-level statistics. Critical for query optimization
--               on indexed access paths. Excludes system databases.
-- 
-- Version     : 1.2.0
-- Date        : 2026-05-16
-- Modificado  : 2026-05-13 - Integración y optimización de motor SQL para Módulo 2
--               2026-05-15 - Integración de TablesV (NoPI/Queue) y blindaje contra vacías
--               2026-05-16 - Implementación de XMLAGG para pivotar columnas y 
--                            eliminar intervenciones manuales en índices sin nombre.
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Tablas_Vacias AS (
    SELECT DatabaseName, TableName
    FROM DBC.StatsV
    WHERE StatsId <> 0
    GROUP BY 1, 2
    HAVING MAX(RowCount) = 0
),
Tablas_Con_Datos AS (
    SELECT ts.DatabaseName, ts.TableName
    FROM DBC.TableSizeV ts
    LEFT JOIN Tablas_Vacias tv
        ON ts.DatabaseName = tv.DatabaseName
       AND ts.TableName = tv.TableName
    WHERE tv.TableName IS NULL
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 10485760 
),
Columnas_Indices AS (
    -- Pivoteamos las filas a columnas usando XMLAGG respetando la posición física del índice
    SELECT 
        DatabaseName, 
        TableName, 
        IndexNumber, 
        IndexType, 
        IndexName,
        TRIM(TRAILING ',' FROM (XMLAGG(TRIM(ColumnName) || ',' ORDER BY ColumnPosition)(VARCHAR(2000)))) AS Lista_Columnas
    FROM DBC.IndicesV
    WHERE IndexType IN ('P', 'S', 'Q', 'J')
      AND DatabaseName NOT IN (
            'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
            'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
            'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
            'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
      )
    GROUP BY 1, 2, 3, 4, 5
)
SELECT DISTINCT 
    i.DatabaseName, 
    i.TableName,
    tb.TableKind,
    i.IndexType,
    i.IndexName,
    i.IndexNumber,
    -- Generación directa de la sentencia usando la lista dinámica de columnas
    'COLLECT STATISTICS COLUMN (' || i.Lista_Columnas || ') ON ' || TRIM(i.DatabaseName) || '.' || TRIM(i.TableName) || ';' AS Action_SQL
FROM Columnas_Indices i
INNER JOIN DBC.TablesV tb
    ON i.DatabaseName = tb.DatabaseName
    AND i.TableName = tb.TableName
INNER JOIN Tablas_Con_Datos t
    ON i.DatabaseName = t.DatabaseName
    AND i.TableName = t.TableName
LEFT JOIN DBC.StatsV s 
    ON i.DatabaseName = s.DatabaseName 
    AND i.TableName = s.TableName 
    AND i.IndexNumber = s.IndexNumber
WHERE s.IndexNumber IS NULL
  AND tb.TableKind IN ('T', 'O', 'Q') 
  AND tb.AuthName is null --Se omitn tablas NOS
ORDER BY i.DatabaseName, i.TableName, i.IndexType;