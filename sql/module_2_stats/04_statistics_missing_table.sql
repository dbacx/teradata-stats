-- =============================================================================
-- Component 04: Missing at Table Level (Active Tables)
-- =============================================================================
-- Description : Identifies tables over 10MB that have ZERO statistics collected,
--               but are actively being queried according to PDCR. Prioritizes 
--               missing statistics by actual system usage (Access count).
--               Generates COLLECT SUMMARY STATISTICS as the safe, executable baseline.
--               Uses DBC.TablesV, DBC.StatsV (Anti-Join) and DBQLObjTbl_Hst.
--               Excludes system databases and foreign tables (NOS).
-- 
-- Version     : 1.2.0
-- Date        : 2026-05-15
-- Modificado  : 2026-05-15 - Reemplazo de alerta por script ejecutable (Summary Stats)
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Tablas_Fisicas AS (
    -- Descartamos vacías y ajustamos el espejismo del Fallback
    SELECT 
        ts.DatabaseName, 
        ts.TableName, 
        SUM(CASE WHEN t.ProtectionType = 'F' THEN ts.CurrentPerm / 2 ELSE ts.CurrentPerm END) AS Real_Data_Bytes
    FROM DBC.TableSizeV ts
    INNER JOIN DBC.TablesV t
        ON ts.DatabaseName = t.DatabaseName
        AND ts.TableName = t.TableName
    WHERE t.TableKind IN ('T', 'O', 'Q')
	AND t.AuthName is null
    GROUP BY 1, 2
    HAVING SUM(ts.CurrentPerm) > 10485760 -- > 10 MB
),
Tablas_Con_Estadisticas AS (
    -- Identificamos las tablas que tienen al menos UNA estadística de columna/índice
    SELECT DatabaseName, TableName
    FROM DBC.StatsV
    WHERE StatsId <> 0
    GROUP BY 1, 2
),
Uso_PDCR AS (
    -- Extraemos el uso real de las tablas en los últimos 30 días
    SELECT ObjectDatabaseName, ObjectTableName, SUM(FreqofUse) AS Accesos_30D
    FROM PDCRINFO.DBQLObjTbl_Hst
    WHERE LogDate BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
      AND ObjectType = 'Tab'
      AND ObjectDatabaseName NOT IN (
          'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
          'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
          'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
          'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
      )
    GROUP BY 1, 2
)
SELECT 
    tf.DatabaseName, 
    tf.TableName,
    CAST(tf.Real_Data_Bytes / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    COALESCE(u.Accesos_30D, 0) AS Accesos_Ultimos_30_Dias,
    -- Generación del comando base universal para sanar el motor
    'COLLECT SUMMARY STATISTICS ON ' || TRIM(tf.DatabaseName) || '.' || TRIM(tf.TableName) || ';' AS Action_SQL
FROM Tablas_Fisicas tf
-- Anti-Join: Solo dejamos las que NO existen en el diccionario de estadísticas
LEFT JOIN Tablas_Con_Estadisticas s
    ON tf.DatabaseName = s.DatabaseName
    AND tf.TableName = s.TableName
-- Cruzamos con el uso para priorizar por gravedad
INNER JOIN Uso_PDCR u
    ON tf.DatabaseName = u.ObjectDatabaseName
    AND tf.TableName = u.ObjectTableName
WHERE s.TableName IS NULL 
  AND u.Accesos_30D > 30 
  AND tf.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
  )
ORDER BY Accesos_Ultimos_30_Dias DESC, Size_GB DESC;