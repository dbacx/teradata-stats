-- =============================================================================
-- Component 02: Sample Candidates (Aligned with Teradata Best Practices)
-- =============================================================================
-- Description : Identifies indexed columns with high uniqueness ratio (>=95% NDV)
--               that should use USING SAMPLE instead of FULL SCAN.
--               Enforces official rules:
--               - Target must be an Index (IndexNumber IS NOT NULL).
--               - Table must not be skewed (AMP Variance <= 5%).
--               - Excludes small tables and Global Temp / Join Indexes.
-- 
-- Version     : 1.2.0
-- Date        : 2026-05-15
-- Modificado  : 2026-05-15 - Integración de reglas de Data Skew y validación de Índices
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================


WITH Analisis_Skew_Fisico AS (
    -- Validamos que la tabla tenga datos y calculamos la varianza entre AMPs
    SELECT 
        DatabaseName, 
        TableName,
        SUM(CurrentPerm) AS TotalPerm,
        -- Cálculo de Varianza Skew: (Max - Avg) / Avg * 100
        CAST((MAX(CurrentPerm) - AVG(CurrentPerm)) AS DECIMAL(18,4)) / NULLIF(AVG(CurrentPerm), 0) * 100 AS Skew_Pct
    FROM DBC.TableSizeV
    GROUP BY 1, 2
    -- Ignoramos headers y vacías (>10MB)
    HAVING SUM(CurrentPerm) > 10485760 
),
Uso_Estadisticas AS (
    -- Extraemos el uso real para garantizar que estamos optimizando estadísticas activas
    SELECT ObjectDatabaseName, ObjectTableName, ObjectColumnName, SUM(FreqofUse) AS Accesos_PDCR
    FROM PDCRINFO.DBQLObjTbl_Hst
    WHERE LogDate BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
      AND ObjectType = 'Idx' -- Solo evaluamos índices según la regla de Teradata
      AND ObjectDatabaseName NOT IN (
          'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
          'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
          'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
          'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
      )
    GROUP BY 1, 2, 3
)
SELECT 
    s.DatabaseName, 
    s.TableName,
    s.ColumnName,
    s.RowCount AS Filas_Tabla,
    s.UniqueValueCount AS Valores_Unicos,
    CAST((CAST(s.UniqueValueCount AS FLOAT) / NULLIF(s.RowCount, 0)) * 100 AS DECIMAL(5,2)) AS Porcentaje_Unicidad,
    'COLLECT STATISTICS USING SYSTEM SAMPLE COLUMN (' || COALESCE(TRIM(s.ColumnName), 'N/A') || ') ON ' || TRIM(s.DatabaseName) || '.' || TRIM(s.TableName) || ';' AS Action_SQL
FROM DBC.StatsV s
INNER JOIN DBC.TablesV t 
    ON s.DatabaseName = t.DatabaseName 
    AND s.TableName = t.TableName
INNER JOIN Analisis_Skew_Fisico sk
    ON s.DatabaseName = sk.DatabaseName
    AND s.TableName = sk.TableName
INNER JOIN Uso_Estadisticas u
    ON s.DatabaseName = u.ObjectDatabaseName
    AND s.TableName = u.ObjectTableName
    AND s.ColumnName = u.ObjectColumnName
WHERE t.TableKind IN ('T', 'O', 'Q') 
  AND s.StatsId <> 0 
  -- REGLA DOC 1: Debe ser un índice, no una columna suelta
  AND s.IndexNumber IS NOT NULL 
  -- REGLA DOC 2: La varianza de skew en los AMPs no puede superar el 5%
  AND sk.Skew_Pct <= 5.00
  -- REGLA DOC 3: Excluir tablas pequeñas (La regla dice filas > AMPs, 100K cubre clústeres modernos)
  AND s.RowCount > 100000 
  -- REGLA DOC 4: Excluir índices con muchos duplicados (Garantizado por NDV >= 95%)
  AND (CAST(s.UniqueValueCount AS FLOAT) / NULLIF(s.RowCount, 0)) >= 0.95 
  AND COALESCE(UPPER(s.ColumnName), '') <> 'PARTITION' 
  AND (s.SampleSignature IS NULL OR UPPER(s.SampleSignature) NOT LIKE 'SDP%') 
  AND s.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
  AND t.AuthName is null
ORDER BY s.RowCount DESC, Porcentaje_Unicidad DESC;