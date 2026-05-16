-- =============================================================================
-- Component 3.03: Suspected Unused Objects
-- =============================================================================
-- Description : Identifies tables > 1GB that have zero recorded accesses 
--               in DBQL over the last 30 days. Candidates for archiving/drop.
-- Version     : 1.0.0
-- Author      : Ricardo Enciso
-- =============================================================================

WITH Tablas_Fisicas AS (
    SELECT DatabaseName, TableName, SUM(CurrentPerm) AS CurrentPerm_Bytes
    FROM DBC.TableSizeV
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 1073741824 -- > 1 GB
),
Uso_PDCR AS (
    SELECT ObjectDatabaseName, ObjectTableName, SUM(FreqofUse) AS Access_Count
    FROM PDCRINFO.DBQLObjTbl_Hst
    WHERE LogDate >= CURRENT_DATE - 30
      AND ObjectType = 'Tab'
    GROUP BY 1, 2
)
SELECT 
    tf.DatabaseName, 
    tf.TableName,
    CAST(tf.CurrentPerm_Bytes / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    COALESCE(u.Access_Count, 0) AS Accesos_30D,
    'DROP TABLE ' || TRIM(tf.DatabaseName) || '.' || TRIM(tf.TableName) || ';' AS Action_SQL
FROM Tablas_Fisicas tf
LEFT JOIN Uso_PDCR u
    ON tf.DatabaseName = u.ObjectDatabaseName
    AND tf.TableName = u.ObjectTableName
WHERE COALESCE(u.Access_Count, 0) = 0
  AND tf.DatabaseName NOT IN ('DBC', 'PDCRDATA', 'TDSTATS')
ORDER BY Size_GB DESC;