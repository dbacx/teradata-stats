-- =============================================================================
-- Component   : Sample Candidates
-- =============================================================================
-- Description : Identifies columns with high uniqueness ratio (>=95% NDV) that
--               should use USING SAMPLE instead of FULL SCAN for statistics
--               collection. Uses DBC.StatsV, DBC.TablesV, and PDCRINFO.DBQLObjTbl_Hst
--               to analyze column usage patterns and recommend optimization actions.
--               Excludes small tables (<100K rows), PARTITION columns, and columns
--               already using dynamic sampling (SDP%).
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Modificado  : 2026-05-13 - Integración y optimización de motor SQL para Módulo 2
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Tablas_Con_Datos AS (
    SELECT DatabaseName, TableName
    FROM DBC.TableSizeV
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 0 
),
Uso_Estadisticas AS (
    SELECT ObjectDatabaseName, ObjectTableName, ObjectColumnName, SUM(FreqofUse) AS Accesos_PDCR
    FROM PDCRINFO.DBQLObjTbl_Hst
    WHERE LogDate BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
      AND ObjectType IN ('Col', 'Idx')
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
    COALESCE(u.Accesos_PDCR, 0) AS Uso_30_Dias,
    s.SampleSizePct,
    s.SampleSignature,
    CASE 
        WHEN COALESCE(u.Accesos_PDCR, 0) = 0 THEN 'DROP STATISTICS COLUMN (' || TRIM(s.ColumnName) || ') ON ' || TRIM(s.DatabaseName) || '.' || TRIM(s.TableName) || ';'
        ELSE 'COLLECT STATISTICS USING SYSTEM SAMPLE COLUMN (' || TRIM(s.ColumnName) || ') ON ' || TRIM(s.DatabaseName) || '.' || TRIM(s.TableName) || ';'
    END AS Action_SQL
FROM DBC.StatsV s
INNER JOIN DBC.TablesV t 
    ON s.DatabaseName = t.DatabaseName 
    AND s.TableName = t.TableName
INNER JOIN Tablas_Con_Datos td
    ON s.DatabaseName = td.DatabaseName
    AND s.TableName = td.TableName
LEFT JOIN Uso_Estadisticas u
    ON s.DatabaseName = u.ObjectDatabaseName
    AND s.TableName = u.ObjectTableName
    AND s.ColumnName = u.ObjectColumnName
WHERE t.TableKind = 'T'
  AND s.StatsId <> 0 
  AND s.RowCount > 100000 
  AND UPPER(s.ColumnName) <> 'PARTITION'
  AND (CAST(s.UniqueValueCount AS FLOAT) / NULLIF(s.RowCount, 0)) >= 0.95
  AND (s.SampleSignature IS NULL OR UPPER(s.SampleSignature) NOT LIKE 'SDP%')
  AND s.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
ORDER BY s.RowCount DESC, Porcentaje_Unicidad DESC;