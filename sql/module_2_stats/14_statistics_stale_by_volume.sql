-- =============================================================================
-- Component 14: Stale by Volume (Data Drift)
-- =============================================================================
-- Description : Detects statistics that are technically "fresh" by date but 
--               obsolete by volume due to massive INSERTs or DELETEs (Data Drift).
--               Uses physical space (CurrentPerm) adjusted for Fallback, divided 
--               by dictionary RowCount to find physically impossible ratios 
--               (< 15 bytes/row or > 30,000 bytes/row).
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-15
-- Modificado  : 2026-05-15 - Ajuste de Fallback y detección bidireccional (Insert/Delete)
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Tablas_Fisicas AS (
    SELECT 
        ts.DatabaseName, 
        ts.TableName, 
        -- Neutralizamos el espejismo del Fallback: Si es 'F', dividimos el espacio a la mitad
        SUM(CASE WHEN t.ProtectionType = 'F' THEN ts.CurrentPerm / 2 ELSE ts.CurrentPerm END) AS Real_Data_Bytes
    FROM DBC.TableSizeV ts
    INNER JOIN DBC.TablesV t
        ON ts.DatabaseName = t.DatabaseName
        AND ts.TableName = t.TableName
    WHERE t.TableKind IN ('T', 'O', 'Q')
	AND t.AuthName is null
    GROUP BY 1, 2
    HAVING SUM(ts.CurrentPerm) > 10485760 -- Omitimos tablas vacías o solo con headers (>10MB)
),
Max_Stats_Rows AS (
    SELECT DatabaseName, TableName, MAX(RowCount) AS Stats_RowCount
    FROM DBC.StatsV
    WHERE StatsId <> 0
    GROUP BY 1, 2
)
SELECT 
    tf.DatabaseName, 
    tf.TableName, 
    CAST(tf.Real_Data_Bytes / (1024.0**3) AS DECIMAL(18,2)) AS Real_Size_GB,
    COALESCE(ms.Stats_RowCount, 0) AS Ultimas_Filas_Reportadas,
    CAST(tf.Real_Data_Bytes / NULLIF(ms.Stats_RowCount, 0) AS DECIMAL(18,2)) AS Pseudo_Bytes_Per_Row,
    CASE 
        WHEN CAST(tf.Real_Data_Bytes / NULLIF(ms.Stats_RowCount, 0) AS DECIMAL(18,2)) < 15 
            THEN 'DRIFT POR DELETE MASIVO'
        ELSE 'DRIFT POR INSERT MASIVO'
    END AS Tipo_De_Desviacion,
    -- Forzamos la actualización completa a nivel de tabla para sanar los histogramas corrompidos
    'COLLECT STATISTICS ON ' || TRIM(tf.DatabaseName) || '.' || TRIM(tf.TableName) || ';' AS Action_SQL
FROM Tablas_Fisicas tf
INNER JOIN DBC.TablesV t 
    ON tf.DatabaseName = t.DatabaseName 
    AND tf.TableName = t.TableName
INNER JOIN Max_Stats_Rows ms
    ON tf.DatabaseName = ms.DatabaseName
    AND tf.TableName = ms.TableName
WHERE ms.Stats_RowCount > 0
  AND (
      -- Condición 1: Extremo Inferior (Físicamente imposible, evidencia de purga masiva de datos)
      CAST(tf.Real_Data_Bytes / NULLIF(ms.Stats_RowCount, 0) AS DECIMAL(18,2)) < 15 
      OR 
      -- Condición 2: Extremo Superior (Evidencia de carga masiva post-recolección de estadísticas)
      CAST(tf.Real_Data_Bytes / NULLIF(ms.Stats_RowCount, 0) AS DECIMAL(18,2)) > 30000
  )
  AND t.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
  AND t.AuthName is null
ORDER BY Real_Size_GB DESC, Pseudo_Bytes_Per_Row DESC;